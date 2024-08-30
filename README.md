# 1brc

Whilst travelling at the start of
2024, [Gunnar's one billion row challenge](https://www.morling.dev/blog/one-billion-row-challenge/) crossed my path.

It _absolutely_ would have been nerd-snipe fodder at the time, but the impracticalities of working on it whilst
travelling were high. I filed it away in [Omnivore](https://omnivore.app) with the intention of giving it a go later.

... time passes ...

![8 months pass](img.png "Omnivore screenshot of the 1brc CTA")

And here we are.

I went in essentially blind, having avoided hackernews/reddit/social media spoilers, aiming for a clean attempt first
then review other solutions to see what I missed.

Gunnar's baseline implementation, running on my machine came in at just under 2 minutes (110,302ms) - having checked the
leaderboard, it was clear to see the top results had somewhat improved on that.

Oh boy 👏

| # | Result (m:s.ms) | Implementation                                                                                                           | JDK          | Submitter                                                                                                                                        | Notes                              | Certificates                                                                                  |
|---|-----------------|--------------------------------------------------------------------------------------------------------------------------|--------------|--------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------|-----------------------------------------------------------------------------------------------|
| 1 | **00:01.535**   | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thomaswue.java) | 21.0.2-graal | [Thomas Wuerthinger](https://github.com/thomaswue), [Quan Anh Mai](https://github.com/merykitty), [Alfonso² Peterssen](https://github.com/mukel) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/thomaswue_merykitty_mukel.pdf) |

## My attempt

* [Click here](README_rjk.md) to see a description of the various iterations
* Ultimately, I stopped at a processing time of just under 4 seconds (**~3,733ms**) - see the link for caveats

## What was learnt

* [Click here](README_review.md) for my notes on other entries
* The bounds-checks on memory segment access are material and aren't optimised out in my usage (UNSAFE still wins)
* I should spend some time looking at Graal, the top of the leardboard is dominated by it
* Avoiding the JVM shutdown costs of mmapped pages by **spawning children to do the work** and **killing the main
  process
  early** is hilarious 👏
* I _wanted_ to use MemorySegments/FFM but beyond bypassing the 2GB limit of ByteBuffers I didn't gain much compared to
  other solutions
* There's a real elegance in the simplicity of the ForkJoin solutions
* VM Optimisations
    * seeing EpsilonGC in the wild was amusing
    * TrustFinalNonStaticFields and UseCountedLoopSafepoints need more investigation for wider usage in my day-to-day
      work

## Final thought

Seriously, just marvel at this from the winning entry:

```java
// Parse a number that may/may not contain a minus sign followed by a decimal with
// 1 - 2 digits to the left and 1 digits to the right of the separator to a
// fix-precision format. It returns the offset of the next line (presumably followed
// the final digit and a '\n')
private static long parseDataPoint(PoorManMap aggrMap, Aggregator node, MemorySegment data, long offset) {
    long word = data.get(JAVA_LONG_LT, offset);
// The 4th binary digit of the ascii of a digit is 1 while
// that of the '.' is 0. This finds the decimal separator
// The value can be 12, 20, 28
    int decimalSepPos = Long.numberOfTrailingZeros(~word & 0x10101000);
    int shift = 28 - decimalSepPos;
// signed is -1 if negative, 0 otherwise
    long signed = (~word << 59) >> 63;
    long designMask = ~(signed & 0xFF);
// Align the number to a specific position and transform the ascii code
// to actual digit value in each byte
    long digits = ((word & designMask) << shift) & 0x0F000F0F00L;

    // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
    // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
    // 0x000000UU00TTHH00 +
    // 0x00UU00TTHH000000 * 10 +
    // 0xUU00TTHH00000000 * 100
    // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
    // This results in our value lies in the bit 32 to 41 of this product
    // That was close :)
    long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
    long value = (absValue ^ signed) - signed;
    aggrMap.observe(node, value);
    return offset + (decimalSepPos >>> 3) + 3;
}
```