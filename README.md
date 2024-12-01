# LSM Implementation built with AI

This is a Log-Structured Merge-tree (LSM) implementation similar to the Golang port of SlateDB, 
an embedded storage engine built on top of object storage. The purpose of this project is for me to
get familiar with LSM and use [Aider](https://aider.chat/) using the most advanced AI models like
claude 3.5 to help build SlateDB, but designed with DDD principles in mind.

## Overview

The goal is to
- Organize the code into small problem domains `wal`, `sstable`, `bloom` etc...
- Define an interface for each of those problem domains by hand
- Instruct the AI to implement interfaces
- Test the implemented interfaces for correctness
- Have the AI fix the implementation when it gets it wrong
- Or Have the AI suggest ways to fix the implementation
- If that fails, then I just ask the AI to borrow SlateDB implementation and adapt it to the new interface

## Results so far

### Block Package
I didn't completely grok that I should write documentation about how the blocks should be laid out on
disk in order for the AI to properly implement this portion. So, instead I had the AI adapt the code from
slateDB.

### Bloom Package
The AI implemented a complete bloom filter package, however it ignored the request to 
`implement enhanced double hashing` which resulted in a significantly different implementation
that what slateDB has. This is fine, but when I ran the tests, all the `HasKey()` tests failed as
the AI wasn't able to figure out how to calculate the filter bits without a reference. I fixed this
and the code worked, but I ended up using the slateDB implementation anyway. =/

Also, the AI did a pretty good job of describing how I should go about diagnosing the bug. In
all I spent about 1 hour on this package, which is pretty fast compared to how long it would have
taken me to write it from scratch, then throw it away and use the slatedb implementation.

### SSTable Package
The AI implemented all the flat buffer encoding and decoding after I finally understood that
the methods I was asking it to implement needed to use `flatbuf.SsTableIndexT` instead of
`flatbuf.SsTableIndex`. The AI had some trouble properly implementing `sstable.Builder.Build()`.
I had to add TODO comments to the code with specific instructions to utilize `encodeIndex` which
the AI wrote a few prompts ago. I also had to come up with a way of storing the block offsets before 
the AI understood that calling `block.Encode()` everytime it needed an offset wasn't an efficient way 
to solve the problem.

