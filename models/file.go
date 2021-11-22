package model

import (
	"encoding/gob"
	"fmt"
	"path"
	"runtime/debug"
	"time"

	"github.com/cloudreve/Cloudreve/v3/pkg/util"
	"github.com/jinzhu/gorm"
	"github.com/luyu6056/cache"
)

// File 文件
type File struct {
	// 表字段
	gorm.Model
	Name       string `gorm:"unique_index:idx_only_one"`
	SourceName string `gorm:"type:text"`
	UserID     uint   `gorm:"index:user_id;unique_index:idx_only_one"`
	Size       uint64
	PicInfo    string
	FolderID   uint `gorm:"index:folder_id;unique_index:idx_only_one"`
	PolicyID   uint

	// 关联模型
	Policy Policy `gorm:"PRELOAD:false,association_autoupdate:false"`

	// 数据库忽略字段
	Position string `gorm:"-"`
}

func init() {
	// 注册缓存用到的复杂结构
	gob.Register(File{})
}

func deleteFileCache(file *File) {

	key := fmt.Sprintf("%d_%s", file.FolderID, file.Name)
	if _, ok := cache.Has(key, "file"); ok {
		cache.Hdel(key, "file")
	}
}

// Create 创建文件记录
func (file *File) Create() (uint, error) {

	if err := DB.Create(file).Error; err != nil {
		util.Log().Warning("无法插入文件记录, %s", err)
		return 0, err
	}
	deleteFileCache(file)
	return file.ID, nil
}

// GetChildFile 查找目录下名为name的子文件
func (folder *Folder) GetChildFile(name string) (*File, error) {

	c := cache.Hget(fmt.Sprintf("%d_%s", folder.ID, name), "file")
	var file *File
	if ok := c.Get("cache", &file); ok {
		if file.ID == 0 {
			return file, gorm.ErrRecordNotFound
		}
		return file, nil
	}
	result := DB.Where("folder_id = ? AND name = ?", folder.ID, name).Find(&file)

	if result.Error == nil {
		file.Position = path.Join(folder.Position, folder.Name)
	}
	if file == nil {
		file = &File{}
	}
	c.Store("cache", &file)
	c.Expire(60)
	return file, result.Error
}

// GetChildFiles 查找目录下子文件
func (folder *Folder) GetChildFiles() ([]File, error) {
	var files []File
	result := DB.Where("folder_id = ?", folder.ID).Find(&files)

	if result.Error == nil {
		for i := 0; i < len(files); i++ {
			files[i].Position = path.Join(folder.Position, folder.Name)
		}
	}
	return files, result.Error
}

// GetFilesByIDs 根据文件ID批量获取文件,
// UID为0表示忽略用户，只根据文件ID检索
func GetFilesByIDs(ids []uint, uid uint) ([]File, error) {
	var files []File
	var result *DBStruct
	if uid == 0 {
		result = DB.Where("id in (?)", ids).Find(&files)
	} else {
		result = DB.Where("id in (?) AND user_id = ?", ids, uid).Find(&files)
	}
	return files, result.Error
}

// GetFilesByKeywords 根据关键字搜索文件,
// UID为0表示忽略用户，只根据文件ID检索
func GetFilesByKeywords(uid uint, keywords ...interface{}) ([]File, error) {
	var (
		files      []File
		result     = DB
		conditions string
	)

	// 生成查询条件
	for i := 0; i < len(keywords); i++ {
		conditions += "name like ?"
		if i != len(keywords)-1 {
			conditions += " or "
		}
	}

	if uid != 0 {
		result = result.Where("user_id = ?", uid)
	}
	result = result.Where("("+conditions+")", keywords...).Find(&files)

	return files, result.Error
}

// GetChildFilesOfFolders 批量检索目录子文件
func GetChildFilesOfFolders(folders *[]Folder) ([]File, error) {
	// 将所有待删除目录ID抽离，以便检索文件
	folderIDs := make([]uint, 0, len(*folders))
	for _, value := range *folders {
		folderIDs = append(folderIDs, value.ID)
	}

	// 检索文件
	var files []File
	result := DB.Where("folder_id in (?)", folderIDs).Find(&files)
	return files, result.Error
}

// GetPolicy 获取文件所属策略
func (file *File) GetPolicy() *Policy {
	if file.Policy.Model.ID == 0 {
		file.Policy, _ = GetPolicyByID(file.PolicyID)
	}
	return &file.Policy
}

// RemoveFilesWithSoftLinks 去除给定的文件列表中有软链接的文件
func RemoveFilesWithSoftLinks(files []File) ([]File, error) {
	// 结果值
	filteredFiles := make([]File, 0)

	// 查询软链接的文件
	var filesWithSoftLinks []File
	tx := DB
	for _, value := range files {
		tx = tx.Or("source_name = ? and policy_id = ? and id != ?", value.SourceName, value.PolicyID, value.ID)
	}
	result := tx.Find(&filesWithSoftLinks)
	if result.Error != nil {
		return nil, result.Error
	}

	// 过滤具有软连接的文件
	// TODO: 优化复杂度
	if len(filesWithSoftLinks) == 0 {
		filteredFiles = files
	} else {
		for i := 0; i < len(files); i++ {
			finder := false
			for _, value := range filesWithSoftLinks {
				if value.PolicyID == files[i].PolicyID && value.SourceName == files[i].SourceName {
					finder = true
					break
				}
			}
			if !finder {
				filteredFiles = append(filteredFiles, files[i])
			}

		}
	}

	return filteredFiles, nil

}

// DeleteFileByIDs 根据给定ID批量删除文件记录
func DeleteFileByIDs(ids []uint) error {
	var files []*File
	if err := DB.Where("id in (?)", ids).Find(&files).Error; err != nil || len(files) == 0 {
		return err
	}
	result := DB.Where("id in (?)", ids).Delete(&File{})
	if result.Error == nil {
		for _, f := range files {
			deleteFileCache(f)
		}
	}
	return result.Error
}

// GetFilesByParentIDs 根据父目录ID查找文件
func GetFilesByParentIDs(ids []uint, uid uint) ([]File, error) {
	files := make([]File, 0, len(ids))
	result := DB.Where("user_id = ? and folder_id in (?)", uid, ids).Find(&files)
	return files, result.Error
}

// Rename 重命名文件
func (file *File) Rename(new string) error {

	if err := DB.Model(&file).Update("name", new).Error; err != nil {
		return err
	}
	deleteFileCache(file)
	return nil
}

// UpdatePicInfo 更新文件的图像信息
func (file *File) UpdatePicInfo(value string) error {

	if err := DB.Model(&file).Set("gorm:association_autoupdate", false).Update("pic_info", value).Error; err != nil {
		return err
	}
	deleteFileCache(file)
	return nil
}

// UpdateSize 更新文件的大小信息
func (file *File) UpdateSize(value uint64) error {
	updateFileSizeChan <- updateFileSizeParam{
		uid:  file.Model.ID,
		size: value,
	}
	key := fmt.Sprintf("%d_%s", file.FolderID, file.Name)
	if c, ok := cache.Has(key, "file"); ok {
		var file *File
		if ok := c.Get("cache", &file); ok {
			file.Size = value
			c.Store("cache", file)
		}
	}
	return nil
}

// UpdateSourceName 更新文件的源文件名
func (file *File) UpdateSourceName(value string) error {

	if err := DB.Model(&file).Set("gorm:association_autoupdate", false).Update("source_name", value).Error; err != nil {
		return err
	}
	deleteFileCache(file)
	return nil
}

/*
	实现 webdav.FileInfo 接口
*/

func (file *File) GetName() string {
	return file.Name
}

func (file *File) GetSize() uint64 {
	return file.Size
}
func (file *File) ModTime() time.Time {
	return file.UpdatedAt
}

func (file *File) IsDir() bool {
	return false
}

func (file *File) GetPosition() string {
	return file.Position
}

type updateFileSizeParam struct {
	uid  uint
	size uint64
}

var updateFileSizeChan = make(chan updateFileSizeParam, 100)

func UpdateFileSize() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("%v\r\n%v", err, string(debug.Stack()))
		}
		go UpdateFileSize()
	}()
	file := &File{}
	for param := range updateFileSizeChan {
		file.Model.ID = param.uid
		DB.Model(&file).Set("gorm:association_autoupdate", false).Update("size", param.size)
	}

}
func init() {
	go UpdateFileSize()
}
