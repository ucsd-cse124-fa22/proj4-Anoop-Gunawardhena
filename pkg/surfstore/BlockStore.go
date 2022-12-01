package surfstore

import (
	context "context"
	"errors"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//fmt.Printf("trying to get block from hash : %s", blockHash.Hash)
	hash := blockHash.Hash
	val, present := bs.BlockMap[hash]
	if present == false {
		fmt.Printf("No block for hash %s", hash)
		return nil, errors.New("No block present")
	}
	//fmt.Println("got block")
	return val, nil
	//panic("todo")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	//panic("todo")
	//fmt.Println("trying to put block")
	hash := GetBlockHashString(block.BlockData)
	bs.BlockMap[hash] = block
	S := &Success{Flag: true}
	//fmt.Println("put block in for hash of %s", hash)
	return S, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	//panic("todo")
	//fmt.Println("starting hashbloks")
	hashIn := []string{}
	if len(blockHashesIn.Hashes) == 0 {
		toreturn := &BlockHashes{Hashes: hashIn}
		fmt.Println("no input hashes so cant exist")
		return toreturn, nil
	}
	for _, h := range blockHashesIn.Hashes {
		_, present := bs.BlockMap[h]
		if present {
			hashIn = append(hashIn, h)
		}
	}
	toreturn := &BlockHashes{Hashes: hashIn}
	//fmt.Println("ended hashblocks")

	return toreturn, nil

}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
