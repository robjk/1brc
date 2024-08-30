Once my attempt was completed, I wanted to look at a few other folks and see their approaches and what could be learnt.

_On the test machine mine was faster than some, slower than others below, relative performance really wasn't the point
though._

- # Review of entries
    - ## 🥇 [Thomas Wuerthinger](https://github.com/thomaswue), [Quan Anh Mai](https://github.com/merykitty), [Alfonso² Peterssen](https://github.com/mukel) 🥇
        - There's an excellent in-depth analysis [here](https://questdb.io/blog/billion-row-challenge-step-by-step/) by
          Marko Topolnik of QuestDB, I wouldn't be able to add anything to it
        - I will just highlight **Quan Anh Mai's** branchless measurement extraction code, it had me grinning from ear
          to ear as I read Marko's explanation of the
          incantations: [dark arts practiced here](https://github.com/gunnarmorling/1brc/blob/db064194be375edc02d6dbcd21268ad40f7e2869/src/main/java/dev/morling/onebrc/CalculateAverage_merykitty.java#L169)

    - ## Richard Startin - [entry](https://github.com/gunnarmorling/1brc/blob/db064194be375edc02d6dbcd21268ad40f7e2869/src/main/java/dev/morling/onebrc/CalculateAverage_richardstartin.java)
        - ### Overview
            - Richard's written some fascinating blog posts generally, you can find
              them [here](https://richardstartin.github.io)

            - Standard ByteBuffer usages
                - So 2GiB limit per mmap/chunk
                - Chunks split in main
                    - Each chunk ends at a record terminator, walks the bytes looking for '\n'
                - Each mmap split into slices of 10 meg for processing
                    - slices similarly end on a '\n'
            - ForkJoin for threading
            - Custom Dictionary (from async-profiler) shared across all tasks
                - Slot records to hold station results, byte[] key, int[] tuple for min/max/sum/count
                - Dictionary works by having a `Row` for each bucket
                - Rows consist of a single Slot and a "next" Table pointer
                - Table contains an array of Row(s) (*and an unused baseIndex*)
            - Single ForkJoin AggregationTask (RecursiveAction, compare with Shipilev's CountedCompleter) to do the work
                - Creates tasks for each slice, with the actual processing performed on a single slice in `computeSlice`
                - Straightforward hunt for a ';' delimiter to get the station name range, then byte by byte extraction
                  of the measurement
            - ### Thoughts vs mine
                - Alternative bit twiddling approach in findIndexOf, similar exception the final OR operation

    - ## Peter Lawrey - [entry](https://github.com/gunnarmorling/1brc/blob/db064194be375edc02d6dbcd21268ad40f7e2869/src/main/java/dev/morling/onebrc/CalculateAverage_lawrey.java)
        - ### Overview
            - He of [Chronical](https://chronicle.software) and [OpenHFT](https://github.com/OpenHFT) fame
            - Minimal 218 lines 👏 easy to understand
            - I was heading down this path (discrete Key template populated with candidate data) before pivoting to a
              custom hashmap implementation
                - Creation of a new Key when a new map value was computed is smarter than my old approach of copying the
                  template into a new key instance
            - It will run into problems with station names > 32 characters
        - ### File access
            - Single RandomAccessFile then MappedByteBuffers per chunk, no unmapping logic
        - ### Threading / Processing
            - Stream parallel, ergo ForkJoin
            - Used both for processing as well as a map/reduce of results
            - Simplest of simple incremental byte reads, switching on specific characters
        - ### Rendering
            - I didn't spend any time looking at the final rendering in mine, but oh boy I should have spotted it was
              just a TreeMap#toString...
        - ### Improvements over mine
            - Easier to understand
            - Just vanilla Java libraries
            - No custom Map
            - Out-of-the-box parallelism

    - ## Aleksey Shipilëv - [entry](https://github.com/gunnarmorling/1brc/blob/db064194be375edc02d6dbcd21268ad40f7e2869/src/main/java/dev/morling/onebrc/CalculateAverage_shipilev.java)
        - What's to say, an extremely smart guy [start here](https://shipilev.net)
        - ### Overview
            - Clean approach, avoiding unsafe and preview
            - Aims to be reliable and saleable without being overly clever
            - Pleasingly simple API design, passes less around than my approach when dealing with the map/entries
        - ### File access
            - ByteBuffers
            - Tidies up mmap'd chunks as each is finished processing (delegates to the main thread to tidy them up)
        - ### Threading / Processing
            - ThreadLocal used to share the result maps, rather than tasks returning results individually
            - Built upon ForkJoin, workers given smaller chunks to work with than mine
            - **CountedCompleter** implementation - *this needs a deeper read* #dev/java/questions
                - **RootTask**
                    - forks a ParsingTask per MMAP_CHUNK_SIZE chunk (tuned by shipilev to an appropriate size)
                    - looks for '\n' from the end of the chunk backwards, surplus becomes part of next chunk
                    - finally creates a new MappedByteBuffer slice to give to the new ParsingTask
                - **ParsingTask** internalCompute subdivides the chunk into smaller chunks until within an acceptable
                  slice size
                    - finds appropriate line endings as part of the subdivision
                    - as per the javadoc, directly **compute** the one task whilst foriking the other
                    - **seqCompute** is used on acceptable sized slices
                        - slices the underlying ByteBuffer and *directly touchs the first byte to allow the compiler to
                          reduce "common" checks*
                        - incremental hash calculation
                        - explicit handling for 4 vs 5 char measurements
                        - reads a single byte to compensate for the sign (if present)
                        - uses the `ascii_byte - '0'` approach to turn ascii into a number, as good as `& 0xf`` ?
                          #dev/java/questions
                        - inlines measurement decode alongside finding the start/end of the station name
        - ### Custom Map
            - Custom Linear probing hash map
            - Slow and fast-path updates
            - comparing the hash for an entry is done outside of the map itself in **seqCompute**,
        - ### Station record
            - int prefixes for faster comparison
            - Hunts for delimiter before creating the Bucket instance
            - Extract logic is within the Bucket constructor, rather than mine which passes it in
            - byte[] for name tail instantiated within the constructor for data locality when scanning later
        - ### Rendering
            - Pojo for individual "Rows" of the result
            - StringBuilder concatenation
            - Uses the main method's spare time whilst it's waiting to setup the rendering/touch required classes for
              preloading
        - ### General performance
            - Prefers conditional update over using Math.min/max
        - ### VM Options
        - A whole slew of VM options suggested for peak performance
          #[[java/learning/VM Options]]
            -
            ```
            -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -Xms1g -Xmx1g -XX:-AlwaysPreTouch -XX:+UseTransparentHugePages
            -XX:-TieredCompilation -XX:-UseCountedLoopSafepoints -XX:+TrustFinalNonStaticFields -XX:CompileThreshold=2048
            --add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
            
            -XX:+UnlockDiagnosticVMOptions -XX:CompileCommand=quiet
            
            -XX:CompileCommand=dontinline,dev.morling.onebrc.CalculateAverage_shipilev\$ParsingTask::seqCompute
            -XX:CompileCommand=dontinline,dev.morling.onebrc.CalculateAverage_shipilev\$MeasurementsMap::updateSlow
            -XX:CompileCommand=inline,dev.morling.onebrc.CalculateAverage_shipilev\$Bucket::matches
            ```
                - Epsilon GC makes an appearance 🙂
                - Disables pretouch, will improve startup time, with subsequent page faults amortised over multiple threads
                - UseTransparentHugePage - makes sense given the nature of the task
                - Disable tiered compilation - go straight for C2, will improve time to reach peak-performance
                - Disable Safepoints in loops, there's less to be done during safepoints in this task so disabling will be a benefit
                - `TrustFinalNonStaticField` this is a nice once, allows for more aggressive compiler optimisations
                - CompileThreshold reduction will result in faster compilation of methods at the expense of statistic gathering, likely not an issue given the predictability of the code
            -
        - ### Improvements over mine
            - dealing with the sign character, i reread the whole int, could have just read the single byte and shifted
              the existing int
            - evaluating hash comparisons is done outside of the map in **seqCompute** as opposed to mine within the
              equals call
            - performs speculative station name comparisons vs ByteBuffer slice, as opposed to my eagerly copying into a
              byte[]
            - main method does useful work whilst it's waiting for workers to complete (rendering code)
            - VM tweaking (as he points out they are optional though)