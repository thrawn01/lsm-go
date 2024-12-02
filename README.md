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

## Results

### Block Package
As this is the first thing I asked the AI to build, I didn't completely grok that I should write detailed
documentation about how the blocks should be laid out on disk in order for the AI to properly implement
this portion. So, instead I had the AI adapt the code from [slatedb-go](https://github.com/slatedb/slatedb-go).

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

#### ReadBlocks()
I had to `/ask` the AI to describe how it would implement the method several times. Each time I had to 
add more `//TODO` comments to the method and update the method comment before it finally gave me what I wanted.
The AI wanted to make multiple calls to the read only blob in order to fetch each block individually.
This was suboptimal as we have no idea if the ReadOnlyBlob implementation is making remote calls to fetch
the offsets, so I had to spell out exactly what I wanted in the TODOs before it would do it.

Once that was done, the AI wrote the code perfectly. The only change I needed to make was in the tests,
as the AI didn't realize that multiple blocks would not result unless the keys exceeded the block size.

# What I've learned
In all, using the AI made me really think hard about how I would explain what I wanted. The advantage of
this is that the code gets better documentation than it normally would if I was writing the implementation
as I know what I want, and don't have to describe it to anyone. This way, the code gets documented and 
I end up moving faster than I normally would. 

The other thing I considered is that I might be able to ask a newer AI model to re-write a method to 
improve it, or make it more efficient. The excellent documentation that results due to process assists
future developers and AI as the documentation clearly spells out what the code "should" be doing and why.

I don't think I could have gotten this far with as little time as I've invested in this without the AI.

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
Provide an implementation of sstable.Decoder.ReadBloom() using
 the same error verbage as ReadInfo().
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

##### For complex methods, Ask the AI to explain first
Use the `/ask` command in aider to ask how the AI would implement a method. This avoids having the AI make
changes the FIX those changes it got wrong because it didn't understand your requirements.

Often times I will make several `/ask` follow-ups after adding `//TODO` or improving the method comment until
it understands what the method is supposed todo.

###### Git Add before asking the AI to make changes
Run aider with `auto-commits: false` and commit your changes before asking the AI to make code changes. This 
allows you to quickly roll back if the AI gets it really wrong.