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
and the code worked, but I ended up using the slateDB implementation because I'm not a bloom filter
expert. =/

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

#### ReadInfo()
The AI implemented the method incorrectly, after a few attempts and diagnosis, I realized the AI wrote the
final SSTable offset as an `uint64` instead of a `uint32` which caused an out-of-bounds error. Once I fixed 
this the method passed the provided test.

#### ReadIndex()
The AI implemented both `ReadIndex()` and `ReadIndexFromBytes()` perfectly, even including negative and
positive tests.

## What I learned

##### Provide exact instructions in the method comments
When designing the methods and functions you want the AI to implement, explicitly state what and how the 
method should operate. This gives the AI hints as to what you expect. Then when prompting include additional
instructions 

Example:
```go
// ReadBloom reads the bloom.Filter from the provided store using blob.ReadRange()
// using the offsets provided by Info.
func (d *Decoder) ReadBloom(info *Info, b ReadOnlyBlob) (*bloom.Filter, error) {
    return nil, nil // TODO
}
```
Prompt:
```
Provide an implementation of sstable.Decoder.ReadBloom() using the same error verbage as ReadInfo().
```

##### Add TODOs to complex methods for the AI to follow
When asking the AI to implement methods which require multiple steps or utilizes different parts of the code base, I
got better results when I added `//TODO` comments in the method. For example:

```go
// Build returns the SSTable in it's encoded form
func (bu *Builder) Build() *Table {

    // TODO: Finalize the last block if it's not empty

    // TODO: Encode blocks using block.Encode()

    // TODO: Build the bloom filter using bu.bloomBuilder.Build() if the number of keys
	//  is greater than bu.conf.MinFilterKeys

    // TODO: Build and encode the flatbuf.SsTableIndexT using bu.blocks[].Meta

    // TODO: Build and encode sstable.Info using encodeInfo() from flatbuf.go

    // TODO: Append the info offset as a uint32
}

```

