package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	toreturn := &FileInfoMap{FileInfoMap: m.FileMetaMap}
	return toreturn, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	//panic("todo")
	name := fileMetaData.Filename
	version := fileMetaData.Version
	filedata, present := m.FileMetaMap[name]
	if present {
		if version == filedata.Version+int32(1) {
			m.FileMetaMap[name] = fileMetaData
			return &Version{Version: fileMetaData.Version}, nil
		} else {
			return &Version{Version: int32(-1)}, nil

		}
	}
	m.FileMetaMap[name] = fileMetaData
	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	//panic("todo")
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
