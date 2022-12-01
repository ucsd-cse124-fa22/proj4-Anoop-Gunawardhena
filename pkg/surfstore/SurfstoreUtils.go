package surfstore

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	addr := new(string)
	err := client.GetBlockStoreAddr(addr)
	fmt.Println("HEre is address" + *addr)
	if err != nil {
		fmt.Printf("gettig blockstoreaddress from metastore failed due to this %s error \n", err)
		os.Exit(1)

	}
	fileList, err := os.ReadDir(client.BaseDir)
	if err != nil {
		fmt.Printf("Scanning Base directory failed due to this %s error \n", err)
		os.Exit(1)

	}
	//Compute the hashlists for each file in base dir

	Dirmetamap, err := ComputeHashLists(fileList, client.BaseDir, client.BlockSize)
	if err != nil {

		fmt.Printf("Computing Hash list failed due to this %s error \n", err)
		os.Exit(1)

	}
	// Creating a Mapping of filenames to metadata of the files in the local index
	localIndex, _ := LoadMetaFromMetaFile(client.BaseDir)
	// Checking if basedir contains updated/altered versions
	for filename, meta := range Dirmetamap {
		metadata, exists := localIndex[filename]
		if !exists {
			meta.Version = int32(1)
			continue
		}

		if equals(meta.BlockHashList, metadata.BlockHashList) == false {
			meta.Version = metadata.Version + int32(1)
			continue
		}
		meta.Version = metadata.Version
	}
	// Get remote index
	RemoteIndexMap := make(map[string]*FileMetaData)
	errs := client.GetFileInfoMap(&RemoteIndexMap)
	if errs != nil {
		fmt.Printf("Could not get the remote index from server due to this %s error \n", err)
		os.Exit(1)
	}
	//Compare base dir with remote index and local index
	//Downloading files that are in the remote index which aren't in local index or base dir
	for filename, meta := range RemoteIndexMap {
		_, exists1 := localIndex[filename]
		_, exists2 := Dirmetamap[filename]
		//Download files not present in Base Dir or local index
		if !exists1 {
			if !exists2 { //not present in either so download blocks

				if len(meta.BlockHashList) == 1 && meta.BlockHashList[0] == "0" { //for when the remote file is a deleted version
					localIndex[filename] = meta

				} else {
					blockArr, filemetadata := DownloadBlocks(filename, client)
					BuildFile(blockArr, filemetadata, client)
					localIndex[filename] = filemetadata
					Dirmetamap[filename] = filemetadata
				}

			}

		}

	}
	//
	for filename, meta := range Dirmetamap {
		value1, exists1 := localIndex[filename]
		value2, exists2 := RemoteIndexMap[filename]

		if !exists1 {
			if !exists2 { //Upload new files not present in local or remote index
				// input := filename + CONFIG_DELIMITER + string(meta.Version) + CONFIG_DELIMITER
				// for _, hash := range meta.BlockHashList {
				// 	input = input + hash + HASH_DELIMITER
				// }
				blockHashout := new([]string)
				err := client.HasBlocks(meta.BlockHashList, *addr, blockHashout)
				if err != nil {
					fmt.Println("Issue with hasblocks")
					fmt.Println(err)
					os.Exit(1)
				}
				UploadBlocksPath := filepath.Join(client.BaseDir, filename)
				//file, err := os.Open(UploadBlocksPath)
				errup1 := UploadBlocks(UploadBlocksPath, client, *addr, *blockHashout)
				if errup1 != nil {
					fmt.Printf("error ocurred when uploading blocks : %s", errup1)
					os.Exit(1)
				}
				//var p *int32
				p := new(int32)
				erris := client.UpdateFile(meta, p)
				if erris != nil {
					fmt.Printf("Could not update remote index of server due to this %s error \n", err)
					os.Exit(1)
				}
				if *p == -1 {
					fmt.Println("local version is outdated")
					newRemoteIndex := new(map[string]*FileMetaData)
					error2 := client.GetFileInfoMap(newRemoteIndex)
					if error2 != nil {
						fmt.Printf("couldn't get remote file index due to this error: %s", error2)
						os.Exit(1)
					}
					newremoteindex := *newRemoteIndex
					metaf := newremoteindex[filename]
					if len(metaf.BlockHashList) == 1 && metaf.BlockHashList[0] == "0" { //for when the remote file is a deleted version
						localIndex[filename] = metaf
						err := os.Remove(filepath.Join(client.BaseDir, filename))
						if err != nil {
							fmt.Printf("couldn't locally delete file due to this error %s", err)
						}

					} else {
						blockArr, filemetadata := DownloadBlocks(filename, client)
						BuildFile(blockArr, filemetadata, client) //GET THE REMOTE VERSION OF FILE HERE (CONFLICT HAS OCCURED)
						localIndex[filename] = filemetadata
						Dirmetamap[filename] = filemetadata
					}
					continue
				}
				fmt.Println("updated remote index")
				localIndex[filename] = meta
				newRemoteIndex := new(map[string]*FileMetaData)
				error2 := client.GetFileInfoMap(newRemoteIndex)
				if error2 != nil {
					fmt.Printf("couldnt get remote index because %s", error2)
					os.Exit(1)
				}
				useme := *newRemoteIndex
				fmt.Print(FileMetaDataToString(useme[filename]))

			} else { // doesnt exist in local index but exists remotely thus is outdated
				fmt.Println("local version is outdated")
				newRemoteIndex := new(map[string]*FileMetaData)
				error2 := client.GetFileInfoMap(newRemoteIndex)
				if error2 != nil {
					fmt.Printf("couldn't get remote file index due to this error: %s", error2)
					os.Exit(1)
				}
				newremoteindex := *newRemoteIndex
				metaf := newremoteindex[filename]
				if len(metaf.BlockHashList) == 1 && metaf.BlockHashList[0] == "0" { //for when the remote file is a deleted version
					localIndex[filename] = metaf
					err := os.Remove(filepath.Join(client.BaseDir, filename))
					if err != nil {
						fmt.Printf("couldn't locally delete file due to this error %s", err)
					}
					continue
				} else {
					blockArr, filemetadata := DownloadBlocks(filename, client)
					BuildFile(blockArr, filemetadata, client) //GET THE REMOTE VERSION OF FILE HERE (CONFLICT HAS OCCURED)
					localIndex[filename] = filemetadata
					Dirmetamap[filename] = filemetadata
					continue
				}

			}
		} else { //eists in the local index(and by extention the remote index)
			if meta.Version == value1.Version {
				//base and local index are both synced
				if value2.Version > value1.Version { //Remote index has a more recent version than local index and basedir

					fmt.Println("local version is outdated")
					newRemoteIndex := new(map[string]*FileMetaData)
					error2 := client.GetFileInfoMap(newRemoteIndex)
					if error2 != nil {
						fmt.Printf("couldn't get remote file index due to this error: %s", error2)
						os.Exit(1)
					}
					newremoteindex := *newRemoteIndex
					metaf := newremoteindex[filename]
					if len(metaf.BlockHashList) == 1 && metaf.BlockHashList[0] == "0" { //for when the remote file is a deleted version
						localIndex[filename] = metaf
						err := os.Remove(filepath.Join(client.BaseDir, filename))
						if err != nil {
							fmt.Printf("couldn't locally delete file due to this error %s", err)
						}
						continue
					} else {
						blockArr, filemetadata := DownloadBlocks(filename, client)
						BuildFile(blockArr, filemetadata, client) //GET THE REMOTE VERSION OF FILE HERE (CONFLICT HAS OCCURED)
						localIndex[filename] = filemetadata
						Dirmetamap[filename] = filemetadata
						continue
					}

				}
			} else { // the file in the base directory has been updated without updating the local index

				//There are changes to file not reflected in local index
				if value2.Version == value1.Version {
					//Both local index and remote index are outdated
					// input := filename + CONFIG_DELIMITER + string(meta.Version) + CONFIG_DELIMITER
					// for _, hash := range meta.BlockHashList {
					// 	input = input + hash + HASH_DELIMITER
					// }
					blockHashout := new([]string)
					err := client.HasBlocks(meta.BlockHashList, *addr, blockHashout)
					if err != nil {
						fmt.Println("Issue with hasblocks")
						fmt.Println(err)
						os.Exit(1)
					}
					UploadBlocksPath := filepath.Join(client.BaseDir, filename)
					//file, err := os.Open(UploadBlocksPath)
					errup2 := UploadBlocks(UploadBlocksPath, client, *addr, *blockHashout)
					if errup2 != nil {
						fmt.Printf("error ocurred when uploading blocks", errup2)
						os.Exit(1)
					}
					p := new(int32)
					erru := client.UpdateFile(meta, p)
					if erru != nil {
						fmt.Printf("Could update remote index of server due to this %s error \n", err)
						os.Exit(1)
					}
					if *p == -1 {
						fmt.Println("local version is outdated")
						newRemoteIndex := new(map[string]*FileMetaData)
						error2 := client.GetFileInfoMap(newRemoteIndex)
						if error2 != nil {
							fmt.Printf("couldn't get remote file index due to this error: %s", error2)
							os.Exit(1)
						}
						newremoteindex := *newRemoteIndex
						metaf := newremoteindex[filename]
						if len(metaf.BlockHashList) == 1 && metaf.BlockHashList[0] == "0" { //for when the remote file is a deleted version
							localIndex[filename] = metaf
							err := os.Remove(filepath.Join(client.BaseDir, filename))
							if err != nil {
								fmt.Printf("couldn't locally delete file due to this error %s", err)
							}
							continue
						} else {
							blockArr, filemetadata := DownloadBlocks(filename, client)
							BuildFile(blockArr, filemetadata, client) //GET THE REMOTE VERSION OF FILE HERE (CONFLICT HAS OCCURED)
							localIndex[filename] = filemetadata
							Dirmetamap[filename] = filemetadata
							continue
						}

					}
					localIndex[filename] = meta

				} else { // This means that the remote index has a higher version than the local index and thus takes priority even if there is a new file in the abse dir
					fmt.Println("local version is outdated")
					newRemoteIndex := new(map[string]*FileMetaData)
					error2 := client.GetFileInfoMap(newRemoteIndex)
					if error2 != nil {
						fmt.Printf("couldn't get remote file index due to this error: %s", error2)
						os.Exit(1)
					}
					newremoteindex := *newRemoteIndex
					metaf := newremoteindex[filename]
					if len(metaf.BlockHashList) == 1 && metaf.BlockHashList[0] == "0" { //for when the remote file is a deleted version
						localIndex[filename] = metaf
						err := os.Remove(filepath.Join(client.BaseDir, filename))
						if err != nil {
							fmt.Printf("couldn't locally delete file due to this error %s", err)
						}
						continue
					} else {
						blockArr, filemetadata := DownloadBlocks(filename, client)
						BuildFile(blockArr, filemetadata, client) //GET THE REMOTE VERSION OF FILE HERE (CONFLICT HAS OCCURED)
						localIndex[filename] = filemetadata
						Dirmetamap[filename] = filemetadata
						continue
					}

				}

			}

		}

	}
	//Syncing deletions
	for filename, meta := range localIndex {
		_, exists := Dirmetamap[filename]
		if !exists {
			//file has been deleted locally
			//check if remote index has been updated
			newRemoteIndex := new(map[string]*FileMetaData)
			error2 := client.GetFileInfoMap(newRemoteIndex)
			if error2 != nil {
				fmt.Printf("couldn't get remote file index due to this error: %s", error2)
				os.Exit(1)
			}
			newremoteindex := *newRemoteIndex
			metaf := newremoteindex[filename]
			if metaf.Version > meta.Version { //Remoteindex has a higher version so download it

				if len(metaf.BlockHashList) == 1 && metaf.BlockHashList[0] == "0" { //for when the remote file is a deleted version
					localIndex[filename] = metaf

					continue
				} else {
					blockArr, filemetadata := DownloadBlocks(filename, client)
					BuildFile(blockArr, filemetadata, client) //GET THE REMOTE VERSION OF FILE HERE (CONFLICT HAS OCCURED)
					localIndex[filename] = filemetadata
					Dirmetamap[filename] = filemetadata
					continue
				}
			} else if metaf.Version == meta.Version {
				// The versions are the same so can delete on remote as well
				if !(len(meta.BlockHashList) == 1 && meta.BlockHashList[0] == "0") {
					//Checking if the file isnt already deleted
					meta.Version = meta.Version + int32(1)
					meta.BlockHashList = []string{"0"}
					p := new(int32)
					err := client.UpdateFile(meta, p)
					if err != nil {
						fmt.Printf("Could update remote index of server due to this %s error \n", err)
						os.Exit(1)
					}
					if *p == int32(-1) {
						//Check if file already deleted
						deleted, remoteversion := checkDeleted(filename, client)
						if deleted {
							localIndex[filename].Version = remoteversion
							localIndex[filename].BlockHashList = []string{"0"}
							continue
						} //file has been updated
						blockArr, filemetadata := DownloadBlocks(filename, client)
						BuildFile(blockArr, filemetadata, client) //GET THE REMOTE VERSION OF FILE HERE (CONFLICT HAS OCCURED)
						localIndex[filename] = filemetadata
						Dirmetamap[filename] = filemetadata
						continue
					}
					localIndex[filename].Version = *p
					localIndex[filename].BlockHashList = []string{"0"}

				} //else the files config on both local and remote say its already deleted

			}

		}
	}
	errorwrite := WriteMetaFile(localIndex, client.BaseDir)
	if errorwrite != nil {
		fmt.Printf("Error writing back into index.txt : %s", errorwrite)
	}

}

func equals(one []string, two []string) bool {
	if len(one) != len(two) {
		return false
	}
	for i, item := range one {
		if item != two[i] {
			return false
		}
	}
	return true
}

// panic("todo")
func checkDeleted(filename string, client RPCClient) (deleted bool, version int32) {
	RemoteIndexMap := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&RemoteIndexMap)
	if strings.Join(RemoteIndexMap[filename].BlockHashList, "") == "0" {
		return true, RemoteIndexMap[filename].Version
	}
	return false, RemoteIndexMap[filename].Version
}
func DownloadBlocks(filename string, client RPCClient) (blockarr []*Block, metadata *FileMetaData) {
	RemoteIndexMap := make(map[string]*FileMetaData)
	err := client.GetFileInfoMap(&RemoteIndexMap)
	if err != nil {
		fmt.Printf("Error getting remotefileindex %s", err)
		os.Exit(1)
	}
	addr := new(string)
	err1 := client.GetBlockStoreAddr(addr)
	if err1 != nil {
		fmt.Printf("Error getting blockstoreaddress %s", err1)
		os.Exit(1)
	}
	blockArr := make([]*Block, 0) //[]*Block{}
	filemetadata := RemoteIndexMap[filename]
	for _, hash := range filemetadata.BlockHashList {
		toUse := new(Block)
		err := client.GetBlock(hash, *addr, toUse)
		if err != nil {
			fmt.Printf("error wile downloading the block %s", err)
			os.Exit(1)
		}
		blockArr = append(blockArr, toUse)

	}
	return blockArr, filemetadata
}
func BuildFile(fileblocks []*Block, metadata *FileMetaData, client RPCClient) {
	newFilePath := filepath.Join(client.BaseDir, metadata.Filename)
	outFD, err := os.Create(newFilePath)
	if err != nil {
		fmt.Printf("Error During creating file in base dir %s", err)
		os.Exit(1)
	}
	for _, block := range fileblocks {
		_, err := outFD.Write(block.BlockData)
		if err != nil {
			fmt.Printf("Error During  Writing Blocks into base dir %s", err)
			os.Exit(1)
		}
	}

	outFD.Close()

}
func ComputeHashLists(list []os.DirEntry, base string, blockSize int) (map[string]*FileMetaData, error) {
	hashlistMap := make(map[string]*FileMetaData)
	//var block [blockSize]byte
	for _, direntry := range list {
		if direntry.Name() == "index.txt" {
			continue
		}
		fmt.Println("filename is " + (direntry.Name()))
		filename := direntry.Name()
		stat, err := os.Stat(filepath.Join(base, filename))
		if err != nil || stat.IsDir() {
			return nil, errors.New("couldnt load file from os.Stat or the file is a dir")
		}
		meta := new(FileMetaData)
		meta.Filename = filename
		if stat.Size() == 0 {
			meta.BlockHashList = []string{}
			hashlistMap[filename] = meta
			continue
		}
		filebytes, err := os.ReadFile(filepath.Join(base, filename))
		if err != nil {
			return nil, err
		}
		j := 0
		arr := []string{}
		quotient := len(filebytes) / blockSize
		for i := 0; i < quotient; i++ {
			arr = append(arr, GetBlockHashString(filebytes[j:j+blockSize]))
			j = j + blockSize

		}
		arr = append(arr, GetBlockHashString(filebytes[j:]))
		// quotient := int(stat.Size() / int64(blockSize))
		// remainder := int(stat.Size() % int64(blockSize))

		// arry, _, err := HashFileBlocks(filepath.Join(base, filename), quotient, remainder, blockSize)
		// if err != nil {
		// 	return nil, err
		// }
		meta.BlockHashList = arr
		hashlistMap[filename] = meta

	}
	return hashlistMap, nil
}
func HashFileBlocks(filepath string, quotient int, remainder int, blockSize int) ([]string, [][]byte, error) {
	arry := []string{}
	//var remain[remainder]byte
	blocks := make([][]byte, 0)
	remain := make([]byte, remainder)
	block := make([]byte, blockSize)
	file, _ := os.Open(filepath)
	reader := bufio.NewReader(file)
	i := 0
	start := 0
	//flag := false
	n := blockSize
	for i < quotient {
		num, err := reader.Read(block[start:n])
		if err != nil {
			return nil, nil, err
		}
		if num < (n - start) {
			start = num + start
		} else {
			start = 0
			blocks = append(blocks, block[0:n])
			arry = append(arry, GetBlockHashString(block))
			i = i + 1

		}
	}
	for {
		num, err := reader.Read(remain[start:])
		if err != nil {
			return nil, nil, err
		}
		if num < (remainder - start) {
			start = num + start
		} else {
			blocks = append(blocks, remain[0:])
			arry = append(arry, GetBlockHashString(remain))
			break

		}

	}
	toreturnhash := []string{}
	for _, item := range arry {
		toreturnhash = append(toreturnhash, item)
	}
	return toreturnhash, blocks, nil
}
func GetOverlap(localfileblocks []string, remotefileblocks []string) (intersectionblocks []int) {
	toreturn := []int{}
	if len(remotefileblocks) != len(localfileblocks) {
		for _, hash := range remotefileblocks {
			for j, localhash := range localfileblocks {
				if localhash == hash {
					toreturn = append(toreturn, j)

				}
			}
		}
		return toreturn
	}
	return nil
}
func UploadBlocks(filepath string, client RPCClient, blockaddress string, presentblocks []string) error {
	stat, err := os.Stat(filepath)
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		fmt.Println("file is empty")
		return nil
		//Figure this out
	}
	upfile, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}
	quotient := len(upfile) / client.BlockSize
	j := 0
	arr := []string{}
	for i := 0; i < quotient; i++ {
		arr = append(arr, GetBlockHashString(upfile[j:j+client.BlockSize]))
		j = j + client.BlockSize
	}
	arr = append(arr, GetBlockHashString(upfile[j:]))
	//arr, blocks, _ := HashFileBlocks(filepath, quotient, remainder, client.BlockSize)
	indexestoavoid := GetOverlap(arr, presentblocks)
	sort.Ints(indexestoavoid)
	helper := 0
	helper1 := true
	k := 0
	if len(indexestoavoid) == 0 {
		helper1 = false
	}
	for i := 0; i < quotient; i++ {
		if (helper1 && helper < len(indexestoavoid)) && indexestoavoid[helper] == i {
			helper = helper + 1
			continue
		}
		touse := new(bool)
		toput := new(Block)
		toput.BlockData = upfile[k : k+client.BlockSize]
		toput.BlockSize = int32(client.BlockSize)
		k = k + client.BlockSize
		erri := client.PutBlock(toput, blockaddress, touse)
		if erri != nil {
			return erri
		}
	}
	touse := new(bool)
	toput := new(Block)
	toput.BlockData = upfile[k:]
	toput.BlockSize = int32(client.BlockSize)
	erri := client.PutBlock(toput, blockaddress, touse)
	if erri != nil {
		return erri
	}

	// for index, block := range blocks {
	// 	if (helper1 && helper < len(indexestoavoid)) && indexestoavoid[helper] == index {
	// 		helper = helper + 1
	// 		continue
	// 	}
	// 	boo := new(bool)
	// 	toput := new(Block)
	// 	toput.BlockData = block
	// 	toput.BlockSize = int32(client.BlockSize)
	// 	erri := client.PutBlock(toput, blockaddress, boo)
	// 	if erri != nil {
	// 		return erri
	// 	}

	// }
	// return nil

	/*remain := make([]byte, remainder)
	block := make([]byte, client.BlockSize)

	file, _ := os.Open(filepath)
	reader := bufio.NewReader(file)
	i := 0
	start := 0
	//flag := false
	n := client.BlockSize
	for i < quotient {
		if helper1 && i == indexestoavoid[helper] {
			helper = helper + 1
			i++
			continue
		}
		num, err := reader.Read(block[start:n])
		if err != nil {
			return err
		}
		if num < (n - start) {
			start = num + start
		} else {
			start = 0
			boo := new(bool)
			client.PutBlock(&Block{BlockData: block, BlockSize: int32(client.BlockSize)}, blockaddress, boo)

			//arry.append(arry,GetBlockHashString(block))
			i++

		}
	}
	use := len(indexestoavoid) - 1
	if use == -1 || indexestoavoid[use] != len(arr)-1 {
		for {
			num, err := reader.Read(remain[start:remainder])
			if err != nil {
				return err
			}
			if num < (remainder - start) {
				start = num + start
			} else {
				oof := new(bool)
				client.PutBlock(&Block{BlockData: remain, BlockSize: int32(remainder)}, blockaddress, oof)
				break

			}

		}
	}
	return nil*/
	return nil
}
