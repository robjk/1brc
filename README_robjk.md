## Source
[CalculateAverage_robjk.java](src/main/java/dev/morling/onebrc/CalculateAverage_robjk.java)

## Caveats

* All executions are on my local **Windows** machine
* No core isolation is attempted so it's comparatively noisy
    * The machine is not doing anything _significant_ during these test runs though
* Elapsed time measurements are the result of eyeballing multiple runs and picking "something in the middle"
* On this machine, windows caching the entire measurements.txt file is fully expected

So, in essence this isn't quite the same as Gunnar's arrangement - my results won't be directly comparable, but it
allows for quick iterative development and is good-enough for zero-stakes entertainment.

## Approach

IntelliJ comes with async-profiler built in, so it's easy to initially follow a workflow of:

1. Profile the code
2. Identify hotspots
3. Optimise
4. Repeat

I stopped at the point I was spending more time in [JITWatch](https://github.com/AdoptOpenJDK/jitwatch) and considering
switching to Linux so I could run `perf`...

### Iteration 1 - MemorySegment and Memory Mapped File (marginal performance change vs baseline, 109,389ms)

* memory map the file, in and of itself this won't have a significant impact on performance but it allows for further
  optimisations

#### Issues

* Station names intro String, slow generates garbage
* MemorySegment#get calls reading individual bytes
* new String for the measurement (as per Station names)
* Double.parseDouble - ouch, just ouch
* HashMap related activity
* Lazy-man MemorySegment -> asSlice -> toArray -> String unusurprisingly has high garbage cost

### Iteration 2 - Custom double parsing (~50% performance improvement, 52,745ms)

* Double.parseDouble was horrible to look at, this needed replacing
    * Measurements read as a long rather than byte by byte
    * Basic bit-shifting before returning a double with the correct measurement in
    * The reading by long meant it was possible for the final measurement read to break the bounds of the input file
        * special cased this with a temporary old-style Double.parseDouble

#### Issues

* hashCode generation for the Station name was a material part of processing time, easy to fix

### Iteration 3 - Incremental hash code calculation (~25% improvement, 38,996ms)

* hash code calculated for the station name as they are read byte by byte

#### Issues

* Fairly balanced amongst the main processing components
* The HashMap usage is painful, poor "design" (nay evolution) choices for the key/value object is reaching a dead-end
    * This should be a addressed by a custom Map implementation, but I really didn't fancy going down that path yet
* So, switching to multi-threaded processing would give us an easy performance boost

### Iteration 4 - Threads (~80% improvement, 8,002ms)

* Break the work up into 8 (cores) evenly(ish) sized segment
* Each worker thread is giving a starting offset, walks forward to the next line delimiter and processes from there
* The worker on the previous segment is allowed to read into the next segment if the final record is truncated
* Oddly, performance could be reduced by another 3s (4,803ms) by introducing a System.out.print in main method
  First thought was that it related to console output initialisation, but I couldn't pin it on that so excluded it
* N.B It was at this stage I properly read the rules of the competition again and realised I'd take some liberties,
  modified the code to be conformant

#### Issues

* HashMap.get is still painful
* `extractMeasurement` call spends half of its time reading the long from the memory segment
* Reading the individual bytes of the station name is comparatively costly

### Iteration 5 - Endians and tidying up (~40% improvement, 4,833ms)

* `extractMeasurement` - why is getting the long taking so, well, long?
    * because I'm using big endian, and _as I should have thought about at the time_ intel is little
* Starting to reduce the size of methods with an eye on inlining possibilities

#### Issues

* Hang about... I'm reading a long for the measurement but it can be at most 5 bytes long (-XX.Y)
* That HashMap key/value mess need addressing

### Iteration 6 - Int based measurement reading ("0%" improvement, 4,690ms)

* "Improvement" is within natural variability of results (typically ~2%), so not considering it an improvement
* The int based measurement decoding _feels_ like it's on the right path, so keeping it in

#### Issues

* Addressing the HashMap looks unavoidable

### Iteration 7 - Custom Map implementation (~10% improvement, 4,220ms)

* Dug out The Art Of Computer Science, a LinearProbingMap looks to be suitable
* Stop calculating the incremental measurement sum as a double - track as an int*10, then convert at rendering time

#### Issues

* #1 profiling hotspot: StationResult.equals - comparing the current station name with map entries is costly
* #2 extractMeasurement - not obvious what to do to improve this further
* #3 MemorySegment.get(byte) for the station name

### Iteration 8 - Read StationNames as ints/improve terminator search (~12% improvement, 3,733ms)

* #1 and #3 above were closely related
* Get a bit _clever_ with how we read/compare station names
    1. read by int for the first two ints of a station name
    2. use some bit-manipulation cleverness to find the terminator in either of these ints
    3. any remainder (i.e. station names > 8 bytes) read into a byte[]
* Storing the first couple of ints allows us to directly compare the first 8 chars of any station quickly
    * Given the majority of station names are over 4 bytes it was worth reading the two ints to allow discrimination up
      to 8 characters
* I was aware you can identify the location of a specific byte within a register using bit manipulation operations
    * This reduces branches and byte-code size, with the aim of improving performance and inlining possibilities

