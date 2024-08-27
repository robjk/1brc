package dev.morling.onebrc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/*
    Development notes

    Theory: MemorySegments should allow us to mmap and parse the file no worse than baseline, open up further optimisations
        - results: marginally faster on a noisy windows machine - profiling highlights the following CPU sinks:
            - MemorySegment to array hit for station name byte[]
            - MemorySegment.get calls to read a byte from the mmap file
            - new String as the precursor to Double.ParseDouble
            - Double.parseDouble
            - HashMap.contains calls (equals)
            - HashMap.get calls (equals)
        - highest cost activities
            1. Double.parseDouble + reading from the MemorySegment -> String
            2. reading the station into an array
            3. HashMap get + HashMap contains
        - garbage creation
            - #1 Double.parseDouble
            - MappedMemorySegment.asSlice / toArray [stationString into template]
            - MappedMemorySegment.asSlice / toArray / toString (parseDouble precursor)

    Second iteration: Custom double parsing
        * remove Double.parse entirely (well, edge-case around very last measurement in the file and bounds)

        Costly activities
            1. turning the station name bytes into an array and generating the hash code is **costly**
            2. HashMap get/contains also costly
            3. reading the station bytes to find the ';'

    Third iteration:
        * incremental hash calculation
        * don't create an array of bytes from the memory segment each time, directly populate an existing template
        * better use of template via breaking encapsulation
        * tweaking measurement conversion

        Costly activities:
            1. HashMap.get (and the StationStat equals method)
            2. #extractMeasurement
            3. HashMap.containsKey (see 1)
            4. MemorySegment.get (byte, when reading the station name)

    Note: measurements.txt was recreated after Third iteration, making comparision with future iterations questionnable.
    However, moving to multiple-threads will be a step change in performance.

    Fourth iteration:
        In iteration 3, ~43% time spent on the HashMap get/contains calls - reluctant to implement a new one right now but it's an obvious win

        Time to throw more threads at the work, this will make a significant change

            * Split the work across multiple threads by giving each of them a segment in the file to process, then collate
                the results together at the end.  This will require logic to identify the location of a nearby '\n' given we
                won't know where they are when we pick a starting offset for a thread

        Notes on implementation
            * start up time and shut down time are not being measured, so currently comparable vs previous
                implementations of my own but not anyone else
            * Stricter adherence to the competition rules (can be configured to support max number of stations, assumes max station name length)
            * As described above, file mmapped into the global arena (avoids memory barriers enforced by shared)
            * 8 threads (as per rules) each given a starting offset at n/8 bytes into the memory region
            * threads check that the previous byte is not a '\n' (= they are at a valid start position)
                if they are not at a valid start position walk forward until next '\n' is found
            * threads process sequentially, and all but the last thread are allowed to slightly overrun to complete a
                partially processed measurement (the thread dealing with the next segment will have skipped it)
            * debug validity checks added, will be removed when chasing the smaller gains
            * oddly, introducing a call to system.out.printf (or println) in a non-critical setup loop consistently
                improves the performance.  profiling suggests that the hashmap getNode performs worse (amongst other)
                without it.  JITWatch didn't immediately through up anything so this is deferred for a later
                optimisation phase.  For now the hack is *not* included as an official result

        Profiling notes
            1. HashMap.get still hurts
            2. extractMeasurement, half the time is reading the long from the memory segment, remainder needs a closer look
            3. reading the individual bytes of the station name

            for 2 above, we're suffering an endian conversion, this can be fixed
            also, JITWatch is highlighting large methods impacting inlining decisions, it may be fruitful to look at
            that

    Fifth iteration:
        Improved measurement decoding / switched endianness for long reads

    Sixth iteration:
        revisited the measurement decode, realised the blindingly obvious fact that I don't need to read a long and can
        perform the full decode by checking for a '-' then reading an int.
        This gets rid of the stupid final measurement edge-case - also checked the generated byte code to confirm that
        easier to read intermediate stages in the calculation are not optimised out by the compiler

        Started to clean up the nastiness around the Map and key/value but it needs something more radical.  I've been
        putting it off since profiling the very first iteration but next it will be time to look at a custom HashMap
        solution

    Seventh iteration:
        custom map implementation from Knuth, replaced the exceptionally nasty key/value situation before with a single
        slot entry to represent the cumulative station measurement
        one map per worker thread, collated at end (collation/display is not optimised, but costs ~1% of runtime)
        minor rearrangements to try and reduce the bytecode size of hot methods to allow for improved inlining
            note: total execution time is the driver still, but some of the above really require verification that they
                  are doing what I expect them to do (i.e JITWatch before/after)
        i confess, i'd hoped for a bigger improvement but the station name comparison is costly
        ditched Arrays.mismatch as it didn't appear to perform as well as a simple iteration over the byte[] (whither intrinsics?)

        Profiling outcome
            1. StationResult.equals (comparing the latest station name bytes with those in the map slots)
            2. extractMeasurement (it's not immediately obvious how to improve this further - more detailed investigation necessary)
            3. MemorySegment.get(BYTE) - i.e reading the station name, alternatives (wider-reads) could be fruitful

        Ideas
            1 & 3 are linked - would wider reads/comparisons for station names perform better than byte-by-byte compare

    Eighth iteration:
        stop reading the station name byte by byte, instead move to 4-byte ints
        most station names are longer than 4 bytes and it proved to be more performant to read the first two ints of
            the station name, and performing int comparisons when required for equality checks (falling back to
            remaining bytes where they're otherwise the same)
        introduce some bit-hackery to search for the end delimiter of the station name, this was a good performance bump
        spent a little time reducing the size of methods to imprive inline possibilities, _apart from_ the new
            StationRecord instances the hot-path is inlined well now

        Ideas
            aligned reads, there's an option of scanning through the data using aligned reads, then bit-manipulation to
                hunt for ';' and '\n'
            Classloading - is that introducing delays
            VM parameter tweaking - vanilla settings so far, compilation parameters perhaps?
            StationResult is comparatively large (fields and methods) - strip this down
            MemorySegment usage: would ByteBuffers work as well/better?
            no effort has been made to improve the final collation and string construction for printing, there are
                definitely options for improvement here
            i'm sure there are costs with mmapping the whole file into ram with equal sized segments/worker thread

        Stopping here:
            I'd like to look at other solutions and see the clever ideas I failed to think of, but that will inevitably
                pollute my thinking
            There are other things I'm interested in doing :)
 */

public class CalculateAverage_rjk {

    // restrict to 3/4 of the cores on my test machine
    public static final int THREAD_COUNT = 8;

    private static final String GUNNAR_OUTPUT = "{Abha=-29.8/18.0/68.1, Abidjan=-27.7/26.0/72.8, Abéché=-18.9/29.4/77.9, Accra=-20.4/26.4/77.9, Addis Ababa=-36.7/16.0/64.6, Adelaide=-30.6/17.3/65.3, Aden=-19.7/29.1/76.9, Ahvaz=-25.9/25.4/72.4, Albuquerque=-39.0/14.0/61.3, Alexandra=-36.6/11.0/61.1, Alexandria=-28.8/20.0/69.0, Algiers=-28.8/18.2/67.8, Alice Springs=-27.1/21.0/67.3, Almaty=-41.9/10.0/62.2, Amsterdam=-40.9/10.2/59.2, Anadyr=-57.8/-6.9/48.1, Anchorage=-46.7/2.8/52.4, Andorra la Vella=-41.3/9.8/60.4, Ankara=-39.3/12.0/62.3, Antananarivo=-33.9/17.9/68.6, Antsiranana=-26.0/25.2/74.1, Arkhangelsk=-49.1/1.3/51.9, Ashgabat=-31.5/17.1/66.2, Asmara=-31.4/15.6/69.0, Assab=-19.9/30.5/83.5, Astana=-47.2/3.5/55.1, Athens=-31.2/19.2/66.8, Atlanta=-29.3/17.0/65.2, Auckland=-32.3/15.2/63.7, Austin=-28.7/20.7/69.4, Baghdad=-35.5/22.8/77.3, Baguio=-30.5/19.5/68.8, Baku=-34.1/15.1/65.9, Baltimore=-34.5/13.1/63.6, Bamako=-23.9/27.8/81.4, Bangkok=-23.7/28.6/77.0, Bangui=-24.2/26.0/79.3, Banjul=-23.1/26.0/77.9, Barcelona=-31.0/18.2/67.4, Bata=-25.1/25.1/75.2, Batumi=-32.9/14.0/64.8, Beijing=-40.0/12.9/60.1, Beirut=-28.3/20.9/72.3, Belgrade=-41.8/12.5/67.4, Belize City=-23.8/26.7/77.5, Benghazi=-32.4/19.9/72.6, Bergen=-42.9/7.7/55.0, Berlin=-39.2/10.3/59.7, Bilbao=-36.1/14.7/61.8, Birao=-24.7/26.5/78.2, Bishkek=-43.8/11.3/62.2, Bissau=-20.8/27.0/74.1, Blantyre=-33.6/22.2/74.6, Bloemfontein=-35.3/15.6/63.2, Boise=-37.3/11.4/60.3, Bordeaux=-35.5/14.2/63.6, Bosaso=-23.9/30.0/82.2, Boston=-42.2/10.9/59.1, Bouaké=-24.7/26.0/74.8, Bratislava=-37.0/10.5/60.7, Brazzaville=-25.5/25.0/75.4, Bridgetown=-23.1/27.0/73.5, Brisbane=-28.6/21.4/73.5, Brussels=-38.0/10.5/61.6, Bucharest=-40.7/10.8/61.3, Budapest=-40.4/11.3/64.6, Bujumbura=-26.1/23.8/78.1, Bulawayo=-37.3/18.9/70.3, Burnie=-40.0/13.1/58.7, Busan=-37.2/15.0/65.5, Cabo San Lucas=-25.4/23.9/72.1, Cairns=-24.4/25.0/71.7, Cairo=-31.9/21.4/70.5, Calgary=-43.5/4.4/54.9, Canberra=-36.0/13.1/69.1, Cape Town=-33.4/16.2/65.5, Changsha=-28.5/17.4/67.3, Charlotte=-32.8/16.1/68.0, Chiang Mai=-26.3/25.8/75.6, Chicago=-47.0/9.8/61.2, Chihuahua=-31.5/18.6/67.4, Chittagong=-25.0/25.9/82.9, Chișinău=-38.3/10.2/58.6, Chongqing=-30.4/18.6/70.2, Christchurch=-45.1/12.2/62.4, City of San Marino=-36.8/11.8/61.0, Colombo=-19.8/27.4/76.8, Columbus=-37.3/11.7/61.4, Conakry=-21.9/26.4/87.1, Copenhagen=-43.9/9.1/58.4, Cotonou=-22.3/27.2/77.6, Cracow=-39.6/9.3/61.2, Da Lat=-31.1/17.9/71.5, Da Nang=-21.7/25.8/72.8, Dakar=-24.4/24.0/71.4, Dallas=-34.6/19.0/69.5, Damascus=-33.3/17.0/68.3, Dampier=-26.8/26.4/75.6, Dar es Salaam=-29.2/25.8/73.4, Darwin=-19.7/27.6/76.0, Denpasar=-27.7/23.7/77.4, Denver=-40.9/10.4/59.0, Detroit=-39.1/10.0/58.1, Dhaka=-23.0/25.9/74.7, Dikson=-59.6/-11.1/38.6, Dili=-26.0/26.6/74.5, Djibouti=-18.4/29.9/77.0, Dodoma=-26.4/22.7/73.7, Dolisie=-22.9/24.0/76.8, Douala=-23.0/26.7/77.2, Dubai=-19.4/26.9/76.1, Dublin=-39.1/9.8/61.4, Dunedin=-37.9/11.1/60.5, Durban=-28.3/20.6/76.9, Dushanbe=-41.0/14.7/68.5, Edinburgh=-42.9/9.3/62.6, Edmonton=-42.8/4.2/52.7, El Paso=-30.9/18.1/66.8, Entebbe=-27.8/21.0/71.4, Erbil=-31.9/19.5/68.4, Erzurum=-45.5/5.1/55.8, Fairbanks=-56.3/-2.3/46.5, Fianarantsoa=-28.7/17.9/70.1, Flores,  Petén=-21.7/26.4/78.4, Frankfurt=-39.2/10.6/60.8, Fresno=-33.9/17.9/77.2, Fukuoka=-32.7/17.0/68.9, Gaborone=-27.6/21.0/71.2, Gabès=-31.3/19.5/67.0, Gagnoa=-23.2/26.0/75.2, Gangtok=-39.3/15.2/68.5, Garissa=-17.7/29.3/81.4, Garoua=-21.2/28.3/80.3, George Town=-20.6/27.9/77.0, Ghanzi=-25.5/21.4/68.7, Gjoa Haven=-66.6/-14.4/37.0, Guadalajara=-29.6/20.9/68.6, Guangzhou=-30.9/22.4/70.9, Guatemala City=-28.5/20.4/69.1, Halifax=-39.5/7.5/57.7, Hamburg=-40.1/9.7/59.8, Hamilton=-45.6/13.8/66.8, Hanga Roa=-29.2/20.5/69.4, Hanoi=-25.9/23.6/75.3, Harare=-30.2/18.4/73.6, Harbin=-45.7/5.0/54.7, Hargeisa=-27.0/21.7/72.3, Hat Yai=-26.4/27.0/77.9, Havana=-22.7/25.2/77.3, Helsinki=-42.6/5.9/58.7, Heraklion=-34.5/18.9/69.0, Hiroshima=-32.3/16.3/68.6, Ho Chi Minh City=-22.0/27.4/84.1, Hobart=-35.4/12.7/62.0, Hong Kong=-23.6/23.3/74.4, Honiara=-20.8/26.5/77.3, Honolulu=-22.7/25.4/75.0, Houston=-29.7/20.8/76.2, Ifrane=-36.6/11.4/63.9, Indianapolis=-41.1/11.8/60.4, Iqaluit=-59.5/-9.3/43.1, Irkutsk=-48.0/1.0/51.8, Istanbul=-40.8/13.9/65.0, Jacksonville=-35.4/20.3/68.9, Jakarta=-25.4/26.7/75.5, Jayapura=-19.6/27.0/75.4, Jerusalem=-29.2/18.3/67.6, Johannesburg=-33.8/15.5/68.7, Jos=-26.1/22.8/73.1, Juba=-21.6/27.8/82.3, Kabul=-36.4/12.1/66.1, Kampala=-29.2/20.0/71.2, Kandi=-20.3/27.7/78.8, Kankan=-22.3/26.5/82.9, Kano=-28.7/26.4/77.3, Kansas City=-36.1/12.5/60.0, Karachi=-23.5/26.0/76.6, Karonga=-25.7/24.4/73.0, Kathmandu=-30.3/18.3/66.7, Khartoum=-18.7/29.9/80.0, Kingston=-20.4/27.4/77.4, Kinshasa=-21.4/25.3/73.1, Kolkata=-22.1/26.7/75.2, Kuala Lumpur=-20.5/27.3/76.1, Kumasi=-22.7/26.0/75.4, Kunming=-31.8/15.7/67.8, Kuopio=-47.6/3.4/53.2, Kuwait City=-23.4/25.7/79.2, Kyiv=-42.7/8.4/58.3, Kyoto=-34.4/15.8/62.5, La Ceiba=-26.4/26.2/77.2, La Paz=-31.9/23.7/72.3, Lagos=-21.7/26.8/77.7, Lahore=-31.9/24.3/73.5, Lake Havasu City=-31.7/23.7/74.1, Lake Tekapo=-37.6/8.7/59.6, Las Palmas de Gran Canaria=-28.3/21.2/68.3, Las Vegas=-28.6/20.3/70.1, Launceston=-40.4/13.1/60.1, Lhasa=-42.0/7.6/64.0, Libreville=-22.0/25.9/71.4, Lisbon=-33.9/17.5/64.0, Livingstone=-26.2/21.8/73.9, Ljubljana=-39.3/10.9/59.0, Lodwar=-17.6/29.3/79.0, Lomé=-24.3/26.9/75.9, London=-46.5/11.3/60.2, Los Angeles=-35.9/18.6/67.1, Louisville=-39.3/13.9/63.2, Luanda=-23.3/25.8/77.1, Lubumbashi=-30.5/20.8/69.7, Lusaka=-28.6/19.9/74.8, Luxembourg City=-43.2/9.3/57.2, Lviv=-48.6/7.8/57.6, Lyon=-43.9/12.5/60.9, Madrid=-37.2/15.0/65.5, Mahajanga=-25.4/26.3/77.6, Makassar=-27.5/26.7/78.6, Makurdi=-21.2/26.0/73.7, Malabo=-21.9/26.3/79.9, Malé=-24.8/28.0/75.9, Managua=-22.4/27.3/75.8, Manama=-23.9/26.5/79.6, Mandalay=-24.8/28.0/79.3, Mango=-21.0/28.1/79.0, Manila=-26.1/28.4/83.6, Maputo=-28.5/22.8/73.8, Marrakesh=-31.4/19.6/79.7, Marseille=-39.5/15.8/64.1, Maun=-28.3/22.4/70.6, Medan=-22.7/26.5/80.3, Mek'ele=-27.9/22.7/71.0, Melbourne=-39.0/15.1/63.7, Memphis=-33.5/17.2/66.5, Mexicali=-26.3/23.1/76.8, Mexico City=-32.8/17.5/67.0, Miami=-21.3/24.9/77.9, Milan=-42.4/13.0/63.2, Milwaukee=-39.5/8.9/57.2, Minneapolis=-46.2/7.8/59.7, Minsk=-47.7/6.7/55.8, Mogadishu=-24.2/27.1/81.7, Mombasa=-22.6/26.3/75.6, Monaco=-32.1/16.4/65.8, Moncton=-45.5/6.1/57.4, Monterrey=-27.5/22.3/75.5, Montreal=-42.5/6.8/58.2, Moscow=-43.7/5.8/55.8, Mumbai=-25.7/27.1/76.0, Murmansk=-50.0/0.6/47.4, Muscat=-23.0/28.0/76.3, Mzuzu=-31.0/17.7/67.0, N'Djamena=-20.4/28.3/83.6, Naha=-26.1/23.1/74.6, Nairobi=-32.7/17.8/68.0, Nakhon Ratchasima=-24.1/27.3/80.7, Napier=-36.4/14.6/62.3, Napoli=-32.0/15.9/64.4, Nashville=-40.3/15.4/67.0, Nassau=-28.1/24.6/78.0, Ndola=-30.0/20.3/69.5, New Delhi=-28.6/25.0/76.0, New Orleans=-29.2/20.7/76.3, New York City=-38.8/12.9/61.3, Ngaoundéré=-31.1/22.0/70.4, Niamey=-20.4/29.3/82.8, Nicosia=-33.8/19.7/67.6, Niigata=-35.7/13.9/62.1, Nouadhibou=-28.3/21.3/73.7, Nouakchott=-27.0/25.7/75.5, Novosibirsk=-52.6/1.7/56.0, Nuuk=-49.9/-1.4/50.8, Odesa=-37.3/10.7/57.9, Odienné=-23.4/26.0/77.4, Oklahoma City=-34.8/15.9/64.3, Omaha=-39.3/10.6/62.7, Oranjestad=-19.1/28.1/78.2, Oslo=-46.1/5.7/58.9, Ottawa=-46.7/6.6/55.1, Ouagadougou=-24.3/28.3/78.4, Ouahigouya=-21.5/28.6/76.1, Ouarzazate=-31.5/18.9/71.7, Oulu=-48.9/2.7/52.5, Palembang=-23.5/27.3/80.8, Palermo=-27.9/18.5/70.2, Palm Springs=-25.3/24.5/78.3, Palmerston North=-36.0/13.2/64.3, Panama City=-22.4/28.0/76.3, Parakou=-28.1/26.8/76.9, Paris=-39.2/12.3/65.9, Perth=-29.8/18.7/66.3, Petropavlovsk-Kamchatsky=-49.8/1.9/49.3, Philadelphia=-35.6/13.2/60.4, Phnom Penh=-22.5/28.3/81.6, Phoenix=-24.0/23.9/74.1, Pittsburgh=-37.5/10.8/59.8, Podgorica=-35.9/15.3/65.4, Pointe-Noire=-22.4/26.1/72.1, Pontianak=-29.8/27.7/79.8, Port Moresby=-22.4/26.9/72.7, Port Sudan=-20.3/28.4/79.8, Port Vila=-22.5/24.3/75.1, Port-Gentil=-26.9/26.0/77.7, Portland (OR)=-39.0/12.4/62.6, Porto=-31.2/15.7/63.2, Prague=-42.6/8.4/61.0, Praia=-26.1/24.4/70.9, Pretoria=-28.9/18.2/67.5, Pyongyang=-38.2/10.8/64.0, Rabat=-30.0/17.2/64.5, Rangpur=-24.2/24.4/72.2, Reggane=-27.1/28.3/75.0, Reykjavík=-42.8/4.3/52.5, Riga=-43.5/6.2/54.1, Riyadh=-24.1/26.0/75.3, Rome=-37.1/15.2/62.9, Roseau=-24.4/26.2/76.3, Rostov-on-Don=-40.2/9.9/60.3, Sacramento=-31.8/16.3/65.6, Saint Petersburg=-45.4/5.8/57.1, Saint-Pierre=-45.6/5.7/55.5, Salt Lake City=-39.4/11.6/67.6, San Antonio=-30.4/20.8/68.8, San Diego=-33.7/17.8/66.6, San Francisco=-34.3/14.6/65.1, San Jose=-30.7/16.4/66.5, San José=-28.9/22.6/74.3, San Juan=-21.4/27.2/77.6, San Salvador=-27.6/23.1/78.6, Sana'a=-27.8/20.0/67.8, Santo Domingo=-23.3/25.9/78.6, Sapporo=-45.2/8.9/64.6, Sarajevo=-37.6/10.1/56.9, Saskatoon=-52.4/3.3/50.7, Seattle=-42.5/11.3/64.1, Seoul=-35.3/12.5/66.7, Seville=-30.9/19.2/76.7, Shanghai=-38.5/16.7/68.0, Singapore=-27.1/27.0/79.2, Skopje=-33.8/12.4/62.7, Sochi=-40.5/14.2/69.1, Sofia=-38.7/10.6/61.6, Sokoto=-25.7/28.0/74.4, Split=-42.3/16.1/64.1, St. John's=-44.9/5.0/56.8, St. Louis=-34.5/13.9/66.1, Stockholm=-44.2/6.6/54.5, Surabaya=-26.5/27.1/75.0, Suva=-25.0/25.6/74.9, Suwałki=-41.4/7.2/59.2, Sydney=-34.8/17.7/73.4, Ségou=-26.1/28.0/79.4, Tabora=-26.4/23.0/72.9, Tabriz=-37.3/12.6/62.8, Taipei=-29.6/23.0/70.7, Tallinn=-42.8/6.4/55.2, Tamale=-22.3/27.9/77.4, Tamanrasset=-32.1/21.7/69.8, Tampa=-28.8/22.9/74.5, Tashkent=-32.0/14.8/62.8, Tauranga=-32.8/14.8/64.1, Tbilisi=-38.0/12.9/62.7, Tegucigalpa=-27.7/21.7/71.3, Tehran=-32.5/17.0/66.5, Tel Aviv=-28.6/20.0/70.8, Thessaloniki=-34.9/16.0/63.2, Thiès=-24.3/24.0/77.9, Tijuana=-31.1/17.8/69.7, Timbuktu=-23.2/28.0/75.3, Tirana=-35.4/15.2/65.3, Toamasina=-29.2/23.4/75.4, Tokyo=-47.1/15.4/64.6, Toliara=-26.4/24.1/75.2, Toluca=-37.1/12.4/64.8, Toronto=-41.4/9.4/57.6, Tripoli=-29.7/20.0/66.6, Tromsø=-51.2/2.9/51.0, Tucson=-33.3/20.9/73.4, Tunis=-30.7/18.4/72.5, Ulaanbaatar=-49.7/-0.4/58.7, Upington=-30.1/20.4/71.1, Vaduz=-38.7/10.1/59.5, Valencia=-35.0/18.3/69.6, Valletta=-31.1/18.8/68.9, Vancouver=-39.9/10.4/58.8, Veracruz=-23.9/25.4/76.3, Vienna=-36.8/10.4/59.4, Vientiane=-29.9/25.9/77.5, Villahermosa=-29.6/27.1/79.6, Vilnius=-41.5/6.0/56.4, Virginia Beach=-35.5/15.8/65.8, Vladivostok=-43.0/4.9/51.9, Warsaw=-38.5/8.5/59.4, Washington, D.C.=-34.6/14.6/68.6, Wau=-22.2/27.8/75.2, Wellington=-35.7/12.9/59.8, Whitehorse=-51.7/-0.1/50.9, Wichita=-34.9/13.9/61.7, Willemstad=-26.6/28.0/83.4, Winnipeg=-44.2/3.0/55.5, Wrocław=-37.0/9.6/58.6, Xi'an=-38.1/14.1/62.2, Yakutsk=-57.3/-8.8/40.2, Yangon=-24.8/27.5/77.8, Yaoundé=-26.3/23.8/75.2, Yellowknife=-54.8/-4.3/50.3, Yerevan=-34.7/12.4/58.9, Yinchuan=-46.5/9.0/56.4, Zagreb=-40.3/10.7/60.7, Zanzibar City=-27.1/26.0/76.0, Zürich=-41.7/9.3/58.4, Ürümqi=-39.1/7.4/56.6, İzmir=-31.8/17.9/73.0}";

    public static final ValueLayout.OfInt INT_UNALIGNED_LE = JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    public static final int NEW_LINE_MSB = '\n' << 24;

    public static final int MAP_SIZE = 32768; // chosen to reduce collisions, don't want to overfit to our measurements
    public static final int POSITION_MASK = MAP_SIZE - 1;
    public static final int HASH_CONSTANT = 11587;
    public static final int MAX_STATION_NAME_LENGTH = 100;

    // Total elapsed times are not rigorous, methodology is to run repeatedly on a Windows machine and take somewhere
    // in the middle of the range for the table below.  The variation in timings over repeated runs is now at the stage
    // more serious approaches to measurement are required.

    // Gunnar baseline: 110,302
    // First          : 109,389             (mmap & offHeap access)
    // Second         :  52,745             (custom parse double)
    // Third          :  38,996             (incremental hash ++)
    // Fourth         :  8,002  (4,803*)    (multi-threaded, * = spurious improvement due to printf, needs explaining)
    // Fifth          :  4,833              (multi-threading tidy-up, improved measurement decode, endianness)
    // Sixth          :  4,690              (measurement decoding optimisation)
    // Seventh        :  4,220              (custom map)
    // Eighth         :  3,733              (4-byte station reading + bit hackery)

    private static class StationResult {
        byte[] bytes;
        long nameLength;
        int firstPrefix;
        int secondPrefix;
        int hash;
        int count;
        int sum;
        int min;
        int max;

        public StationResult(byte[] bytes,
                             int bytesLength,
                             long nameLength,
                             int firstPrefix,
                             int secondPrefix,
                             int hash,
                             int measurement) {
            this.bytes = Arrays.copyOf(bytes, bytesLength);
            this.nameLength = nameLength;
            this.firstPrefix = firstPrefix;
            this.secondPrefix = secondPrefix;
            this.hash = hash;
            this.count = 1;
            this.sum = this.min = this.max = measurement;
        }

        // only used for display purposes at the end
        public StationResult(StationResult other) {
            this.bytes = Arrays.copyOf(other.bytes, other.bytes.length);
            this.nameLength = other.nameLength;
            this.firstPrefix = other.firstPrefix;
            this.secondPrefix = other.secondPrefix;
            this.hash = other.hash;
            this.count = other.count;
            this.sum = other.sum;
            this.min = other.min;
            this.max = other.max;
        }

        public void recordMeasurement(int measurement) {
            count++;
            sum += measurement;
            min = Math.min(min, measurement);
            max = Math.max(max, measurement);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public boolean equals(int otherHash,
                              long otherNameLength,
                              int otherFirstPrefix,
                              int otherSecondPrefix,
                              byte[] nameBuffer) {

            // prefixes can always be checked, they'll be 0 if the name is not long enough
            if (hash == otherHash
                    && nameLength == otherNameLength
                    && firstPrefix == otherFirstPrefix
                    && secondPrefix == otherSecondPrefix) {

                for (int i = 0; i < bytes.length; i++) {
                    if (bytes[i] != nameBuffer[i]) {
                        return false;
                    }
                }

                return true;
            }

            return false;
        }

        @Override
        public String toString() {
            double mean = sum / 10.0 / count;

            return "%s=%.1f/%.1f/%.1f".formatted(stationName(), round(min / 10.0), round(mean), round(max / 10.0));
        }

        // lingering around to aid debugging, ultimately will disappear into toString
        private String stationName() {
            if (nameLength < 4) {
                return new String(bytes, StandardCharsets.UTF_8);
            }

            byte[] utf8Bytes = new byte[(int) nameLength];

            utf8Bytes[0] = (byte) (firstPrefix & 0xff);
            utf8Bytes[1] = (byte) (firstPrefix >> 8 & 0xff);
            utf8Bytes[2] = (byte) (firstPrefix >> 16 & 0xff);
            utf8Bytes[3] = (byte) (firstPrefix >> 24 & 0xff);
            int index = 4;

            if (nameLength >= 8) {
                utf8Bytes[4] = (byte) (secondPrefix & 0xff);
                utf8Bytes[5] = (byte) (secondPrefix >> 8 & 0xff);
                utf8Bytes[6] = (byte) (secondPrefix >> 16 & 0xff);
                utf8Bytes[7] = (byte) (secondPrefix >> 24 & 0xff);
                index = 8;
            }

            if (bytes.length > 0) {
                System.arraycopy(bytes, 0, utf8Bytes, index, bytes.length);
            }

            return new String(utf8Bytes, StandardCharsets.UTF_8);
        }
    }

    private static class LinearProbingHashMap {
        /**
         * Digging out Knuth's Art of Computer Science leads to discussion of a number of hashing algorithms.  The
         * linear probe looks to be a good fit.  Simple to implement and as long as occupancy is kept low then it's
         * performant.
         */
        StationResult[] table = new StationResult[MAP_SIZE];

        public void merge(byte[] bytes,
                          long nameLength,
                          int bytesLength,
                          int firstPrefix,
                          int secondPrefix,
                          int hash,
                          StationResult probedEntry,
                          int measurement) {

            // we tried and found an empty slot before calling in, short-circuit out
            if (probedEntry == null) {
                // empty slot, create a new entry and we're done
                table[hash & POSITION_MASK] = new StationResult(bytes,
                        bytesLength,
                        nameLength,
                        firstPrefix,
                        secondPrefix,
                        hash,
                        measurement);
                return;
            }

            // we know the current slot is not null and does not match, so advance to next slot before further probing
            int position = (hash + 1) & POSITION_MASK;

            while (true) {
                StationResult entry = table[position];

                // optimistic assumption that there is already something in the slot
                if (entry != null && entry.equals(hash, nameLength, firstPrefix, secondPrefix, bytes)) {
                    entry.recordMeasurement(measurement);
                    return;
                }

                if (entry == null) {
                    // empty slot, create a new entry and we're done
                    table[position] = new StationResult(bytes,
                            bytesLength,
                            nameLength,
                            firstPrefix,
                            secondPrefix,
                            hash,
                            measurement);
                    return;
                }

                // missed, probe the next slot
                position = (position + 1) & POSITION_MASK;
            }
        }

        // the following methods are used for final display, not part of the critical execution path
        public void merge(StationResult update) {
            int position = update.hash & POSITION_MASK;

            while (true) {
                StationResult entry = table[position];
                if (entry == null) {
                    // empty slot, copy the current station result and we're done
                    table[position] = new StationResult(update);
                    return;
                }

                if (entry.hash == update.hash) {
                    if (entry.equals(update.hash,
                            update.nameLength,
                            update.firstPrefix,
                            update.secondPrefix,
                            update.bytes)) {
                        entry.count += update.count;
                        entry.sum += update.sum;
                        entry.min = Math.min(entry.min, update.min);
                        entry.max = Math.max(entry.max, update.max);
                        return;
                    }
                }
                position = (position + 1) & POSITION_MASK;
            }
        }

        public void merge(LinearProbingHashMap otherMap) {
            Arrays.stream(otherMap.table)
                    .filter(Objects::nonNull)
                    .forEach(this::merge);
        }

        public String toString() {
            return "{" +
                    Arrays.stream(table)
                            .filter(Objects::nonNull)
                            .sorted(Comparator.comparing(StationResult::toString))
                            .map(StationResult::toString)
                            .collect(Collectors.joining(", "))
                    + "}";
        }
    }

    @SuppressWarnings("preview")
    private static class Worker implements Callable<LinearProbingHashMap> {

        private final LinearProbingHashMap stationResults;
        private final MemorySegment source;
        private final long maxOffset;

        private long sourceOffset;

        public Worker(MemorySegment source, long startingOffset, long maxOffset) {
            this.source = source;
            this.sourceOffset = startingOffset;
            this.maxOffset = maxOffset;
            this.stationResults = new LinearProbingHashMap();
        }

        @Override
        public LinearProbingHashMap call() {
            try {
                byte[] bytes = new byte[MAX_STATION_NAME_LENGTH];
                int bytesIndex = 0;
                int hash;
                long stationStart = sourceOffset;

                int firstPrefixWord, secondPrefixWord;

                while (true) {
                    // annoyingly, the station name could be 1 character so there's a risk if we read a long we'll
                    // out-of-bounds the final measurement in the file ('a' ';' 'x' '.' 'y' '\n' = 6 bytes)
                    firstPrefixWord = source.get(JAVA_INT_UNALIGNED, sourceOffset);

                    if (delimiterTrailingZeros(firstPrefixWord) == 32) {
                        // first prefix is fine, attempt to read the second prefix, cannot OOB on this read
                        // ';' 'x' '.' 'y' '\n' = 5 bytes
                        sourceOffset += 4;
                        hash = (HASH_CONSTANT * firstPrefixWord);

                        secondPrefixWord = source.get(JAVA_INT_UNALIGNED, sourceOffset);
                        if (delimiterTrailingZeros(secondPrefixWord) == 32) {
                            // second prefix also fine
                            sourceOffset += 4;
                            hash ^= (HASH_CONSTANT * secondPrefixWord);
                        } else {
                            secondPrefixWord = 0;
                        }
                    } else {
                        firstPrefixWord = 0;
                        secondPrefixWord = 0;
                        hash = 0;
                    }

                    // read remainder into the byte[]
                    while (true) {
                        byte lastChar = source.get(JAVA_BYTE, sourceOffset++);
                        if (lastChar == ';') {
                            break;
                        }

                        bytes[bytesIndex++] = lastChar;
                        hash ^= HASH_CONSTANT * lastChar;
                    }

                    long nameLength = sourceOffset - stationStart - 1;

                    int measurement = extractMeasurement(source);

                    // try a quick lookup, if we get lucky can directly update and continue
                    StationResult entry = stationResults.table[hash & POSITION_MASK];
                    if (entry != null && entry.equals(hash,
                            nameLength,
                            firstPrefixWord,
                            secondPrefixWord,
                            bytes)) {
                        entry.recordMeasurement(measurement);
                    } else {
                        // quick lookup failed, take the slow path
                        stationResults.merge(bytes,
                                nameLength,
                                bytesIndex,
                                firstPrefixWord,
                                secondPrefixWord,
                                hash,
                                entry,
                                measurement);
                    }

                    if (sourceOffset >= maxOffset) {
                        break;
                    }

                    bytesIndex = 0;
                    stationStart = sourceOffset;
                }
            } catch (Exception e) {
                System.err.println("failed: " + e);
                throw new RuntimeException(e);
            }

            return stationResults;
        }


        private static int delimiterTrailingZeros(int firstPrefixWord) {
            // bit-bashing to hunt for ';'
            // mask any sign bits (UTF8 chars may have them, ';' does not)
            // xor each byte with ';' -> all existing ';' bytes will now be 0
            // add the max signed byte value 0x7f to each byte, non-';' bytes will overflow to -ve
            // invert, only ';' bytes will have their MSB still set
            // and with a MSB mask to zero out non ';' bytes
            // only bytes that contained ';' will have their msb set
            return Integer.numberOfTrailingZeros(-((firstPrefixWord & 0x7f7f7f7f ^ 0x3b3b3b3b) + 0x7f7f7f7f) & 0x80808080);
        }

        /**
         * decode the measurement, first check for a '-', then use specialisations to decode the remaining scenarios.
         *
         * @param source source buffer
         * @return the measurement value * 10, defer dividing by 10 to display logic
         */
        private int extractMeasurement(MemorySegment source) {

            int negativeMultiplier = 1;
            // i'm *assuming* a byte read followed by an integer read is not a significant performance hit but this has
            // not been verified...  You'd expect everything we need would already be in a cache line so it's just the
            // get overhead we're paying.
            // Profiling suggests the int read is 1/3rd of the execution time and the byte is 1/6th.
            // I might return to reading a long and shifting instead later
            if (source.get(JAVA_BYTE, sourceOffset) == '-') {
                negativeMultiplier = -1;
                sourceOffset++;
            }

            // max possible remaining characters in the measurement is 4 ('x' 'x' '.' 'y' ['\n']), fits in an int
            int measurementInt = source.get(INT_UNALIGNED_LE, sourceOffset);

            // performing the full decode in a compound statement avoids intervening ISTOREs in the byte code
            if ((measurementInt & 0xFF000000) == NEW_LINE_MSB) {
                sourceOffset += 4;
                // single digit integer components      x.y\n
                return ((measurementInt & 0xF) * 10
                        + (measurementInt >> 16 & 0xF))
                        * negativeMultiplier;
            } else {
                sourceOffset += 5;
                // 2 integer digits     xx.y\n
                return ((measurementInt & 0xf) * MAX_STATION_NAME_LENGTH
                        + (measurementInt >> 8 & 0xF) * 10
                        + (measurementInt >> 24 & 0xF))
                        * negativeMultiplier;
            }
        }
    }

    void main() {
        Timer timer = new Timer();

        Arena offHeap = Arena.global();

        try (FileChannel channel = FileChannel.open(Path.of("D:\\development\\workspace\\1brc\\measurements.txt"), StandardOpenOption.READ)) {

            MemorySegment source = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), offHeap);

            long segmentSize = source.byteSize() / THREAD_COUNT;
            long startingOffset = 0;

            Future<LinearProbingHashMap>[] futures = new Future[THREAD_COUNT];

            for (int i = 0; i < THREAD_COUNT - 1; i++) {
                long modifiedStart = adjustStartOffset(startingOffset, source);
                long maxOffset = startingOffset + segmentSize;

                futures[i] = startWorker(source, modifiedStart, maxOffset);
                startingOffset += segmentSize;
            }
            // kick off the final worker
            futures[THREAD_COUNT - 1] = startWorker(source, adjustStartOffset(startingOffset, source), source.byteSize());

            String result = collateResults(futures);
            System.out.println(result);

            // complete - print elapsed time
            System.out.printf("\n%s%n", timer);

            // verify output is as expected
            validityCheck(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void validityCheck(String result) {
        // validity checks
        if (!result.equals(GUNNAR_OUTPUT)) {
            System.out.println("expected: " + GUNNAR_OUTPUT);
            System.out.println("actual  : " + result);
            System.out.print("          ");
            for (int i = 0; i < Math.min(GUNNAR_OUTPUT.length(), result.length()); i++) {
                if (GUNNAR_OUTPUT.charAt(i) != result.charAt(i)) {
                    System.out.print("^");
                } else {
                    System.out.print("-");
                }
            }
        }
    }

    private static long adjustStartOffset(long startingOffset, MemorySegment source) {
        long modifiedStart = startingOffset;

        // walk forward (if required) until we find the start a new station record
        if (modifiedStart != 0 && source.get(JAVA_BYTE, modifiedStart - 1) != (byte) '\n') {
            // we're aligned to the start of a station name '\n', find the next one and start from there
            while (source.get(JAVA_BYTE, modifiedStart) != (byte) '\n') {
                modifiedStart++;
            }
            modifiedStart++;
        }
        return modifiedStart;
    }

    private static FutureTask<LinearProbingHashMap> startWorker(MemorySegment source,
                                                                long startingOffset,
                                                                long maxOffset) {
        Worker worker = new Worker(source, startingOffset, maxOffset);
        var futureTask = new FutureTask<>(worker);
        new Thread(futureTask, "worker-from-" + worker.sourceOffset).start();
        return futureTask;
    }

    private static String collateResults(Future<LinearProbingHashMap>[] futures) {
        LinearProbingHashMap first = uncheckedGet(futures[0]);

        Arrays.stream(futures)
                .toList()
                .stream()
                .skip(1)
                .map(CalculateAverage_rjk::uncheckedGet)
                .forEach(first::merge);

        return first.toString();
    }

    private static <T> T uncheckedGet(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
