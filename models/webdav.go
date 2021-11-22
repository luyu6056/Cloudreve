package model

import (
	"strconv"

	"github.com/jinzhu/gorm"
	"github.com/luyu6056/cache"
)

// Webdav 应用账户
type Webdav struct {
	gorm.Model
	Name     string // 应用名称
	Password string `gorm:"unique_index:password_only_on"` // 应用密码
	UserID   uint   `gorm:"unique_index:password_only_on"` // 用户ID
	Root     string `gorm:"type:text"`                     // 根目录
}

// Create 创建账户
func (webdav *Webdav) Create() (uint, error) {
	if err := DB.Create(webdav).Error; err != nil {
		return 0, err
	}
	return webdav.ID, nil
}

// GetWebdavByPassword 根据密码和用户查找Webdav应用
func GetWebdavByPassword(password string, uid uint) (*Webdav, error) {
	webdav := &Webdav{}
	c := cache.Hget(strconv.Itoa(int(uid)), "Webdav")
	if ok := c.Get(password, &webdav); ok {
		return webdav, nil
	}
	res := DB.Where("user_id = ? and password = ?", uid, password).First(webdav)
	if webdav.ID > 0 {
		c.Set(password, webdav)
	}
	return webdav, res.Error
}

// ListWebDAVAccounts 列出用户的所有账号
func ListWebDAVAccounts(uid uint) []Webdav {
	var accounts []Webdav
	DB.Where("user_id = ?", uid).Order("created_at desc").Find(&accounts)
	return accounts
}

// DeleteWebDAVAccountByID 根据账户ID和UID删除账户
func DeleteWebDAVAccountByID(id, uid uint) {
	cache.Hdel(strconv.Itoa(int(uid)), "Webdav")
	DB.Where("user_id = ? and id = ?", uid, id).Delete(&Webdav{})
}
