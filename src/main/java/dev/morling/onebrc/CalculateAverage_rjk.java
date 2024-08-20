package dev.morling.onebrc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.BIG_ENDIAN;

/*
    Development notes

    Theory: MemorySegments should allow us to mmap and parse the file no worse than baseline, open up further optimisations
        - results: marginally faster on a noisy windows machine - profiling highlights the following CPU sinks:
            - MemorySegment to array hit for city name byte[]
            - MemorySegment.get calls to read a byte from the mmap file
            - new String as the precursor to Double.ParseDouble
            - Double.parseDouble
            - HashMap.contains calls (equals)
            - HashMap.get calls (equals)
        - highest cost activities
            1. Double.parseDouble + reading from the MemorySegment -> String
            2. reading the city into an array
            3. HashMap get + HashMap contains
        - garbage creation
            - #1 Double.parseDouble
            - MappedMemorySegment.asSlice / toArray [cityString into template]
            - MappedMemorySegment.asSlice / toArray / toString (parseDouble precursor)

    Second iteration: Custom double parsing
        * remove Double.parse entirely (well, edge-case around very last measurement in the file and bounds)

        Costly activities
            1. turning the city name bytes into an array and generating the hash code is **costly**
            2. HashMap get/contains also costly
            3. reading the city bytes to find the ';'

    Third iteration:
        * incremental hash calculation
        * don't create an array of bytes from the memory segment each time, directly populate an existing template
        * better use of template via breaking encapsulation
        * tweaking measurement conversion

        Costly activities:
            1. HashMap.get (and the CityStat equals method)
            2. #extractMeasurement
            3. HashMap.containsKey (see 1)
            4. MemorySegment.get (byte, when reading the city name)

    Fourth iteration:
        ~43% time spent on the HashMap get/contains calls - reluctant to implement a new one right now but it's an obvious win
        Time to throw more threads at the work, this should make a significant different

 */

public class CalculateAverage_rjk {

    private static final String GUNNAR_OUTPUT = "{Abha=-36.6/18.0/63.8, Abidjan=-23.7/26.0/79.4, Abéché=-22.2/29.4/78.6, Accra=-21.7/26.4/77.9, Addis Ababa=-32.7/16.0/64.6, Adelaide=-29.1/17.3/64.9, Aden=-20.1/29.1/81.4, Ahvaz=-24.5/25.4/75.4, Albuquerque=-33.5/14.0/64.6, Alexandra=-41.2/11.0/62.0, Alexandria=-29.3/20.0/71.2, Algiers=-29.5/18.2/66.4, Alice Springs=-28.4/21.0/69.8, Almaty=-40.6/10.0/57.9, Amsterdam=-39.3/10.2/60.4, Anadyr=-57.9/-6.9/48.6, Anchorage=-46.4/2.8/52.3, Andorra la Vella=-39.4/9.8/61.4, Ankara=-37.2/12.0/65.0, Antananarivo=-33.2/17.9/67.2, Antsiranana=-25.4/25.2/76.7, Arkhangelsk=-54.7/1.3/53.7, Ashgabat=-40.7/17.1/71.2, Asmara=-34.9/15.6/68.6, Assab=-19.0/30.5/81.6, Astana=-45.9/3.5/52.9, Athens=-32.4/19.2/69.8, Atlanta=-32.0/17.0/70.1, Auckland=-32.7/15.2/64.6, Austin=-29.2/20.7/75.4, Baghdad=-27.9/22.8/73.6, Baguio=-30.5/19.5/74.0, Baku=-41.3/15.1/65.0, Baltimore=-37.7/13.1/61.2, Bamako=-22.5/27.8/76.3, Bangkok=-24.8/28.6/79.3, Bangui=-21.9/26.0/74.4, Banjul=-25.6/26.0/74.3, Barcelona=-33.7/18.2/67.1, Bata=-31.4/25.1/73.3, Batumi=-34.1/14.0/63.1, Beijing=-34.4/12.9/61.4, Beirut=-31.2/20.9/68.7, Belgrade=-38.0/12.5/64.2, Belize City=-22.3/26.7/77.9, Benghazi=-34.5/19.9/68.5, Bergen=-40.2/7.7/57.2, Berlin=-39.7/10.3/60.5, Bilbao=-37.9/14.7/66.5, Birao=-24.6/26.5/77.5, Bishkek=-38.4/11.3/68.6, Bissau=-22.8/27.0/76.4, Blantyre=-29.6/22.2/72.0, Bloemfontein=-31.9/15.6/69.0, Boise=-39.2/11.4/62.2, Bordeaux=-38.6/14.2/65.7, Bosaso=-22.6/30.0/79.4, Boston=-40.5/10.9/60.3, Bouaké=-23.0/26.0/74.9, Bratislava=-36.2/10.5/60.1, Brazzaville=-24.2/25.0/72.7, Bridgetown=-22.0/27.0/78.1, Brisbane=-26.4/21.4/70.4, Brussels=-37.5/10.5/58.3, Bucharest=-37.8/10.8/59.7, Budapest=-42.0/11.3/60.7, Bujumbura=-29.4/23.8/72.5, Bulawayo=-29.1/18.9/66.1, Burnie=-37.2/13.1/60.9, Busan=-34.9/15.0/64.3, Cabo San Lucas=-21.5/23.9/72.4, Cairns=-23.2/25.0/72.1, Cairo=-31.8/21.4/74.7, Calgary=-49.0/4.4/59.4, Canberra=-33.8/13.1/64.7, Cape Town=-35.4/16.2/67.4, Changsha=-33.2/17.4/67.0, Charlotte=-35.7/16.1/63.7, Chiang Mai=-21.5/25.8/74.7, Chicago=-45.0/9.8/62.0, Chihuahua=-28.3/18.6/69.7, Chittagong=-22.0/25.9/81.1, Chișinău=-36.0/10.2/64.9, Chongqing=-32.1/18.6/64.4, Christchurch=-36.9/12.2/60.1, City of San Marino=-37.2/11.8/62.4, Colombo=-21.8/27.4/77.0, Columbus=-38.8/11.7/61.9, Conakry=-25.3/26.4/77.8, Copenhagen=-39.9/9.1/59.1, Cotonou=-22.4/27.2/84.6, Cracow=-40.8/9.3/59.3, Da Lat=-32.4/17.9/69.6, Da Nang=-23.6/25.8/72.6, Dakar=-25.5/24.0/77.3, Dallas=-25.7/19.0/71.0, Damascus=-34.5/17.0/67.1, Dampier=-24.0/26.4/75.5, Dar es Salaam=-20.6/25.8/84.9, Darwin=-25.3/27.6/79.9, Denpasar=-27.5/23.7/71.2, Denver=-37.3/10.4/62.4, Detroit=-42.9/10.0/60.8, Dhaka=-21.8/25.9/77.7, Dikson=-58.5/-11.1/38.7, Dili=-21.2/26.6/75.7, Djibouti=-20.5/29.9/78.3, Dodoma=-25.2/22.7/70.4, Dolisie=-27.6/24.0/74.4, Douala=-20.4/26.7/79.5, Dubai=-21.2/26.9/74.9, Dublin=-39.2/9.8/58.0, Dunedin=-39.2/11.1/59.9, Durban=-28.7/20.6/70.2, Dushanbe=-33.5/14.7/65.5, Edinburgh=-41.7/9.3/58.6, Edmonton=-44.7/4.2/51.8, El Paso=-31.6/18.1/64.0, Entebbe=-28.5/21.0/69.6, Erbil=-37.5/19.5/72.2, Erzurum=-45.0/5.1/55.5, Fairbanks=-54.6/-2.3/47.6, Fianarantsoa=-34.6/17.9/70.6, Flores,  Petén=-24.3/26.4/81.7, Frankfurt=-35.7/10.6/62.4, Fresno=-36.3/17.9/66.8, Fukuoka=-32.6/17.0/62.6, Gaborone=-32.2/21.0/69.2, Gabès=-31.3/19.5/69.4, Gagnoa=-22.7/26.0/82.2, Gangtok=-34.7/15.2/63.8, Garissa=-25.3/29.3/76.8, Garoua=-23.8/28.3/80.6, George Town=-23.7/27.9/79.3, Ghanzi=-27.3/21.4/71.7, Gjoa Haven=-62.6/-14.4/38.2, Guadalajara=-31.9/20.9/72.9, Guangzhou=-26.7/22.4/69.1, Guatemala City=-29.3/20.4/70.8, Halifax=-41.4/7.5/57.9, Hamburg=-41.4/9.7/61.1, Hamilton=-36.1/13.8/65.8, Hanga Roa=-28.8/20.5/69.7, Hanoi=-27.3/23.6/74.1, Harare=-32.4/18.4/65.7, Harbin=-49.7/5.0/61.4, Hargeisa=-25.5/21.7/70.2, Hat Yai=-19.9/27.0/81.2, Havana=-33.1/25.2/74.5, Helsinki=-49.1/5.9/57.6, Heraklion=-32.4/18.9/75.0, Hiroshima=-36.0/16.3/70.5, Ho Chi Minh City=-22.6/27.4/74.1, Hobart=-38.7/12.7/62.6, Hong Kong=-29.7/23.3/74.4, Honiara=-24.4/26.5/79.7, Honolulu=-23.9/25.4/73.2, Houston=-27.4/20.8/70.4, Ifrane=-38.1/11.4/60.2, Indianapolis=-35.2/11.8/62.9, Iqaluit=-55.2/-9.3/44.5, Irkutsk=-49.3/1.0/49.1, Istanbul=-34.5/13.9/60.6, Jacksonville=-32.1/20.3/69.8, Jakarta=-26.5/26.7/75.1, Jayapura=-23.7/27.0/82.6, Jerusalem=-30.3/18.3/68.4, Johannesburg=-35.6/15.5/69.4, Jos=-24.4/22.8/82.7, Juba=-21.0/27.8/76.0, Kabul=-37.3/12.1/63.0, Kampala=-27.9/20.0/67.2, Kandi=-27.3/27.7/78.3, Kankan=-22.0/26.5/76.2, Kano=-23.9/26.4/76.1, Kansas City=-34.5/12.5/64.9, Karachi=-23.8/26.0/74.6, Karonga=-26.3/24.4/73.9, Kathmandu=-30.3/18.3/70.1, Khartoum=-18.8/29.9/81.4, Kingston=-28.4/27.4/75.4, Kinshasa=-24.4/25.3/73.6, Kolkata=-23.0/26.7/74.8, Kuala Lumpur=-21.6/27.3/81.4, Kumasi=-31.9/26.0/77.8, Kunming=-33.9/15.7/61.3, Kuopio=-43.7/3.4/54.7, Kuwait City=-23.1/25.7/74.8, Kyiv=-43.8/8.4/57.5, Kyoto=-32.8/15.8/67.1, La Ceiba=-27.6/26.2/76.7, La Paz=-28.7/23.7/73.1, Lagos=-20.5/26.8/78.7, Lahore=-24.4/24.3/73.5, Lake Havasu City=-25.2/23.7/72.7, Lake Tekapo=-39.7/8.7/58.6, Las Palmas de Gran Canaria=-32.4/21.2/69.6, Las Vegas=-27.7/20.3/71.8, Launceston=-37.9/13.1/60.0, Lhasa=-40.7/7.6/57.3, Libreville=-22.6/25.9/77.2, Lisbon=-34.9/17.5/63.9, Livingstone=-25.1/21.8/70.8, Ljubljana=-42.9/10.9/60.4, Lodwar=-19.5/29.3/82.9, Lomé=-23.3/26.9/75.5, London=-38.6/11.3/61.2, Los Angeles=-29.5/18.6/69.6, Louisville=-35.0/13.9/61.8, Luanda=-24.5/25.8/74.4, Lubumbashi=-33.2/20.8/71.5, Lusaka=-31.3/19.9/68.1, Luxembourg City=-40.8/9.3/58.1, Lviv=-41.1/7.8/56.7, Lyon=-35.0/12.5/60.9, Madrid=-33.3/15.0/64.5, Mahajanga=-27.1/26.3/75.3, Makassar=-21.7/26.7/75.6, Makurdi=-23.8/26.0/76.4, Malabo=-24.2/26.3/75.5, Malé=-20.4/28.0/79.3, Managua=-23.8/27.3/75.2, Manama=-24.2/26.5/76.6, Mandalay=-20.7/28.0/77.1, Mango=-21.5/28.1/76.0, Manila=-20.3/28.4/83.9, Maputo=-27.7/22.8/74.5, Marrakesh=-27.4/19.6/70.6, Marseille=-32.0/15.8/65.0, Maun=-24.8/22.4/72.4, Medan=-23.3/26.5/75.3, Mek'ele=-25.8/22.7/75.2, Melbourne=-32.7/15.1/65.6, Memphis=-32.0/17.2/70.4, Mexicali=-33.0/23.1/75.6, Mexico City=-32.9/17.5/68.6, Miami=-24.3/24.9/73.1, Milan=-37.5/13.0/66.0, Milwaukee=-43.3/8.9/60.7, Minneapolis=-38.4/7.8/56.5, Minsk=-45.2/6.7/58.1, Mogadishu=-20.5/27.1/76.5, Mombasa=-25.2/26.3/78.5, Monaco=-33.4/16.4/68.3, Moncton=-45.6/6.1/59.7, Monterrey=-26.3/22.3/75.8, Montreal=-43.8/6.8/59.4, Moscow=-45.2/5.8/57.7, Mumbai=-21.1/27.1/76.2, Murmansk=-46.4/0.6/51.2, Muscat=-19.7/28.0/78.1, Mzuzu=-30.6/17.7/69.5, N'Djamena=-23.6/28.3/78.4, Naha=-27.4/23.1/71.8, Nairobi=-33.3/17.8/67.6, Nakhon Ratchasima=-24.3/27.3/74.4, Napier=-36.2/14.6/63.0, Napoli=-37.3/15.9/71.0, Nashville=-32.1/15.4/64.0, Nassau=-24.8/24.6/72.1, Ndola=-29.1/20.3/76.5, New Delhi=-25.5/25.0/76.0, New Orleans=-26.4/20.7/72.4, New York City=-36.4/12.9/63.8, Ngaoundéré=-25.6/22.0/78.2, Niamey=-19.8/29.3/87.8, Nicosia=-29.1/19.7/68.4, Niigata=-41.0/13.9/66.7, Nouadhibou=-27.2/21.3/68.9, Nouakchott=-27.9/25.7/78.3, Novosibirsk=-48.2/1.7/54.9, Nuuk=-48.3/-1.4/54.4, Odesa=-47.9/10.7/60.3, Odienné=-24.2/26.0/76.9, Oklahoma City=-33.2/15.9/66.3, Omaha=-45.1/10.6/60.0, Oranjestad=-22.2/28.1/81.3, Oslo=-42.9/5.7/56.7, Ottawa=-41.7/6.6/53.8, Ouagadougou=-19.0/28.3/82.8, Ouahigouya=-22.4/28.6/77.9, Ouarzazate=-32.0/18.9/71.0, Oulu=-49.7/2.7/55.0, Palembang=-20.7/27.3/78.5, Palermo=-30.6/18.5/73.5, Palm Springs=-21.3/24.5/74.3, Palmerston North=-36.0/13.2/65.5, Panama City=-20.8/28.0/76.1, Parakou=-23.8/26.8/78.1, Paris=-38.5/12.3/70.3, Perth=-30.3/18.7/68.1, Petropavlovsk-Kamchatsky=-48.2/1.9/51.7, Philadelphia=-35.4/13.2/59.7, Phnom Penh=-27.4/28.3/76.4, Phoenix=-29.8/23.9/75.8, Pittsburgh=-37.9/10.8/58.8, Podgorica=-34.5/15.3/63.8, Pointe-Noire=-25.3/26.1/74.5, Pontianak=-29.2/27.7/74.2, Port Moresby=-25.5/26.9/75.8, Port Sudan=-19.7/28.4/85.0, Port Vila=-26.0/24.3/75.6, Port-Gentil=-27.3/26.0/84.8, Portland (OR)=-43.3/12.4/62.3, Porto=-33.1/15.7/63.3, Prague=-39.6/8.4/55.5, Praia=-22.9/24.4/72.9, Pretoria=-34.4/18.2/68.3, Pyongyang=-43.1/10.8/63.2, Rabat=-32.1/17.2/67.7, Rangpur=-23.7/24.4/72.4, Reggane=-20.9/28.3/77.5, Reykjavík=-45.4/4.3/54.5, Riga=-44.3/6.2/55.4, Riyadh=-23.5/26.0/79.4, Rome=-35.8/15.2/66.7, Roseau=-22.4/26.2/76.5, Rostov-on-Don=-39.5/9.9/58.6, Sacramento=-31.1/16.3/66.4, Saint Petersburg=-42.1/5.8/53.4, Saint-Pierre=-44.8/5.7/56.5, Salt Lake City=-41.1/11.6/65.5, San Antonio=-27.2/20.8/70.1, San Diego=-30.7/17.8/66.2, San Francisco=-35.7/14.6/62.3, San Jose=-36.9/16.4/66.6, San José=-29.7/22.6/74.3, San Juan=-22.8/27.2/76.6, San Salvador=-32.1/23.1/75.8, Sana'a=-28.3/20.0/69.5, Santo Domingo=-21.3/25.9/77.3, Sapporo=-39.4/8.9/62.4, Sarajevo=-43.0/10.1/62.3, Saskatoon=-47.9/3.3/53.7, Seattle=-42.0/11.3/66.1, Seoul=-35.9/12.5/62.3, Seville=-32.7/19.2/69.8, Shanghai=-29.8/16.7/69.0, Singapore=-24.4/27.0/77.6, Skopje=-39.7/12.4/62.6, Sochi=-35.6/14.2/66.9, Sofia=-39.4/10.6/59.8, Sokoto=-21.9/28.0/80.1, Split=-32.9/16.1/65.4, St. John's=-47.1/5.0/55.8, St. Louis=-40.0/13.9/62.3, Stockholm=-42.4/6.6/55.7, Surabaya=-20.7/27.1/79.3, Suva=-25.6/25.6/73.8, Suwałki=-42.5/7.2/59.8, Sydney=-29.3/17.7/69.8, Ségou=-21.2/28.0/79.9, Tabora=-33.4/23.0/73.4, Tabriz=-38.3/12.6/64.1, Taipei=-30.3/23.0/74.4, Tallinn=-47.6/6.4/56.6, Tamale=-23.8/27.9/79.8, Tamanrasset=-29.2/21.7/72.0, Tampa=-24.5/22.9/71.3, Tashkent=-38.0/14.8/66.1, Tauranga=-35.2/14.8/63.1, Tbilisi=-36.7/12.9/67.0, Tegucigalpa=-26.0/21.7/72.8, Tehran=-34.0/17.0/71.5, Tel Aviv=-30.5/20.0/75.3, Thessaloniki=-34.7/16.0/64.4, Thiès=-26.0/24.0/74.0, Tijuana=-29.9/17.8/69.0, Timbuktu=-27.1/28.0/75.4, Tirana=-35.9/15.2/67.9, Toamasina=-24.8/23.4/76.2, Tokyo=-33.3/15.4/66.6, Toliara=-26.9/24.1/71.3, Toluca=-37.9/12.4/59.8, Toronto=-43.5/9.4/59.4, Tripoli=-30.9/20.0/69.3, Tromsø=-45.3/2.9/53.3, Tucson=-26.8/20.9/70.5, Tunis=-29.8/18.4/69.0, Ulaanbaatar=-50.1/-0.4/49.3, Upington=-30.4/20.4/71.1, Vaduz=-39.1/10.1/61.2, Valencia=-35.2/18.3/69.6, Valletta=-29.2/18.8/67.7, Vancouver=-41.6/10.4/61.1, Veracruz=-25.2/25.4/78.8, Vienna=-39.2/10.4/61.0, Vientiane=-23.8/25.9/78.4, Villahermosa=-18.5/27.1/74.3, Vilnius=-47.6/6.0/58.1, Virginia Beach=-35.8/15.8/67.9, Vladivostok=-43.3/4.9/54.2, Warsaw=-41.4/8.5/59.7, Washington, D.C.=-36.1/14.6/66.9, Wau=-18.6/27.8/80.4, Wellington=-40.9/12.9/62.9, Whitehorse=-52.5/-0.1/50.2, Wichita=-34.2/13.9/64.3, Willemstad=-25.7/28.0/83.6, Winnipeg=-50.9/3.0/50.4, Wrocław=-39.2/9.6/59.4, Xi'an=-34.7/14.1/61.9, Yakutsk=-57.2/-8.8/41.6, Yangon=-23.4/27.5/77.7, Yaoundé=-29.8/23.8/75.5, Yellowknife=-53.8/-4.3/48.3, Yerevan=-36.2/12.4/68.3, Yinchuan=-44.1/9.0/58.5, Zagreb=-39.4/10.7/61.6, Zanzibar City=-24.8/26.0/75.2, Zürich=-37.5/9.3/62.8, Ürümqi=-46.3/7.4/54.5, İzmir=-32.8/17.9/67.1}";

    // gunnar's baseline
    // best = 110,302ms

    // Gunnar baseline: 110,302
    // First          : 109,389     (mmap & offHeap access)
    // Second         :  52,745     (custom parse double)
    // Third          :  38,996     (incremental hash ++)
    // Fourth         :

    /**
     * utility class for rough & ready timing
     */
    public static class Timer {
        private long start;
        private long end;

        public Timer() {
            start();
        }

        public void start() {
            start = System.nanoTime();
        }

        public void end() {
            end = System.nanoTime();
        }

        @Override
        public String toString() {
            end();
            return "%d ms".formatted(TimeUnit.NANOSECONDS.toMillis(end - start));
        }
    }

    /**
     * Second, rewrite the Double parsing logic to avoid creating garbage or using the build in Java methods
     */
    @SuppressWarnings("preview")
    private static class IncrementalHashCalculation {

        public static final ValueLayout.OfLong LONG_UNALIGNED_BE = JAVA_LONG.withOrder(BIG_ENDIAN).withByteAlignment(1);
        public static final long LONG_MASK_MINUS_FIRST_BYTE = (long) '-' << (7 * 8);
        private long sourceOffset;
        // 52 cities
        private final Map<CityStat, CityStat> stats = HashMap.newHashMap(52);

        private static class CityStat {
            byte[] bytes;
            int length;
            int hash;

            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            int measurementCount = 0;
            double cumulative = 0;
            String summaryString = null;

            public CityStat() {
                // constructor only used once to create a resusable template that can hold any city name
                this.bytes = new byte[27];
            }

            /**
             * Copy constructor, populate a new instance from a template
             *
             * @param template pre-populated with the bytes of a city name and the corresponding hash code
             */
            @SuppressWarnings("CopyConstructorMissesField")
            public CityStat(CityStat template) {
                this.bytes = Arrays.copyOf(template.bytes, template.bytes.length);
                this.length = template.length;
                this.hash = template.hash;
            }

            /**
             * @param measurement update this city with a new measurement
             */
            public void add(double measurement) {
                measurementCount++;
                cumulative += measurement;
                min = Math.min(min, measurement);
                max = Math.max(max, measurement);
            }

            private String cityString() {
                return new String(bytes, 0, length, StandardCharsets.UTF_8);
            }

            @Override
            public boolean equals(Object o) {
                if (o instanceof CityStat cityStat && length == cityStat.length) {
                    return Arrays.mismatch(bytes, 0, length, cityStat.bytes, 0, length) < 0;
                }

                return false;
            }

            @Override
            public int hashCode() {
                return hash;
            }

            @Override
            public String toString() {
                if (summaryString == null) {
                    summaryString = "%s=%.1f/%.1f/%.1f".formatted(cityString(), min, cumulative / measurementCount, max);
                }
                return summaryString;
            }
        }

        public String compute(MemorySegment source) {
            sourceOffset = 0;
            byte lastChar;

            CityStat template = new CityStat();
            int length = 0;
            int hash = 0;

            long sourceSize = source.byteSize();

            while (sourceOffset <  sourceSize
                    && (lastChar = source.get(JAVA_BYTE, sourceOffset++)) != (byte) '\n') {

                template.bytes[length++] = lastChar;
                // incrementally calculate the hash code
                hash = 31 * hash + lastChar;

                if (lastChar == ';') {
                    // the ';' is not part of the name
                    template.length = --length;
                    template.hash = hash;

                    // somewhat astonishingly the following  works:
                    // CityStat cityStat = stats.computeIfAbsent(template, CityStat::new);
                    // even though all values will share the same key object - and it's faster...
                    //
                    // I suspect the buckets are sparsely populated and every element in the bucket would need an equals
                    // comparison anyway - however it's likely all hell will break loose on a resize
                    CityStat cityStat = stats.get(template);
                    if (!stats.containsKey(template)) {
                        cityStat = new CityStat(template);
                        stats.put(cityStat, cityStat);
                    }

                    double measurement = extractMeasurement(source);
                    cityStat.add(measurement);

                    length = 0;
                    hash = 0;
                }
            }

            return STR."{\{stats.values().stream()
                    .sorted(Comparator.comparing(CityStat::cityString))
                    .map(CityStat::toString)
                    .collect(Collectors.joining(", "))}}";
        }

        /**
         * All measurements fit within a single long, rather than read bytes individually read the long then bit-shift
         * and mask to identify the '-' and '.'.
         * <p>
         * Allowable specialisations based upon source data:
         * - first char must be a '-' or a number
         * - all numbers have a '.'
         * - one number always follows the '.'
         *
         * @param source source buffer
         * @return the parsed double
         */
        private double extractMeasurement(MemorySegment source) {

            // the very last measurement in the file may not fit into a long without busting the MemorySegment size, as
            // it's a one off at the very end, we take the hit of doing it the easy way
            if (sourceOffset + 8 >= source.byteSize()) {
                String finalMeasurement = new String(source.asSlice(sourceOffset, source.byteSize() - sourceOffset - 1).toArray(JAVA_BYTE), StandardCharsets.UTF_8);
                return Double.parseDouble(finalMeasurement);
            }

            int accumulator = 0;
            double doubleValue;
            int byteOffsetInLong = 7;   // read left to right

            long measurementBytes = source.get(LONG_UNALIGNED_BE, sourceOffset);

            boolean isNegative = (measurementBytes & LONG_MASK_MINUS_FIRST_BYTE) == LONG_MASK_MINUS_FIRST_BYTE;
            if (isNegative) {
                // make a note and skip to next byte in the measurement
                byteOffsetInLong--;
            }

            // could unroll this loop given there are only a handful of possibilities where the decimal place can be.
            // Lower hanging fruit remains available
            while (true) {

                // get next byte in the measurement string
                byte measurementByte = (byte) ((measurementBytes >> (8 * (byteOffsetInLong))) & 0x7F);

                // all measurements are to 1 decimal place so we can finalise and stop once the decimal place is found
                if (measurementByte == '.') {
                    byte lastByte = (byte) (measurementBytes >> 8 * (byteOffsetInLong - 1) & 0x7F);
                    lastByte -= '0';
                    doubleValue = accumulator;
                    doubleValue += (lastByte / 10.0);

                    // advance sourceOffset by the integer component + 3 bytes for '.' + 'number' + '\n'
                    sourceOffset += (7 + 3 ) - byteOffsetInLong;

                    if (isNegative) {
                        doubleValue = -doubleValue;
                    }

                    return doubleValue;
                } else {
                    // otherwise accumulate the integer components
                    accumulator *= 10;
                    accumulator += measurementByte - '0';
                }

                byteOffsetInLong--;
            }
        }
    }

    void main() {

        try (FileChannel channel = FileChannel.open(Path.of("D:\\development\\workspace\\1brc\\measurements.txt"), StandardOpenOption.READ);
             Arena offHeap = Arena.ofConfined()) {

            MemorySegment source = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), offHeap);

            Timer timer = new Timer();

            String result = new IncrementalHashCalculation().compute(source);

            if (!result.equals(GUNNAR_OUTPUT)) {
                System.err.println(GUNNAR_OUTPUT);
                System.err.println(result);
                throw new RuntimeException("did not match");
            }

            System.out.println("\n" + timer);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
