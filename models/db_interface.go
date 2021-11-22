package model

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/cloudreve/Cloudreve/v3/mysql"
	"github.com/jinzhu/gorm"
)

type DBStruct struct {
	gormDb  *gorm.DB
	mysqlDb *mysql.MysqlDB
	Error   error
	slave   bool

	build *mysql.Mysql_Build
}

type DBRow interface {
	Scan(...interface{})
}

func (db *DBStruct) clone() *DBStruct {
	if db.slave == false {
		build := mysql.New_mysqlBuild()
		build.Reset(db.mysqlDb)
		newDB := &DBStruct{mysqlDb: db.mysqlDb, slave: true, build: build, gormDb: db.gormDb}
		build.Db2 = newDB
		return newDB
	}
	return db
}
func (db *DBStruct) AutoMigrate(values ...interface{}) {
	db.gormDb.AutoMigrate(values...)
	//解析struct成员与数据库列名称对应关系
	db.mysqlDb.StructKeyColumn = make(map[string]map[string]*mysql.Field_struct)

	for _, v := range values {
		r := reflect.TypeOf(v).Elem()
		db.mysqlDb.StructKeyColumn[r.Name()] = make(map[string]*mysql.Field_struct)
		for i := 0; i < r.NumField(); i++ {
			field := r.Field(i)
			gormTag := field.Tag.Get("gorm")
			if gormTag == "-" || strings.Contains(gormTag, "save_associations:false") || strings.Contains(gormTag, "association_autoupdate:false") {
				continue
			}
			if field.Name == "Model" && field.Type.String() == "gorm.Model" {
				ID, _ := field.Type.FieldByName("ID")
				db.mysqlDb.StructKeyColumn[r.Name()][field.Name] = &mysql.Field_struct{
					ColumnName: "id",
					Offset:     field.Offset,
					Field_t:    ID.Type,
				}
				db.mysqlDb.StructKeyColumn[r.Name()]["id"] = db.mysqlDb.StructKeyColumn[r.Name()][field.Name]
				//忽略gorm.Model三种类型
				db.mysqlDb.StructKeyColumn[r.Name()]["created_at"] = &mysql.Field_struct{
					ColumnName: "created_at",
				}
				db.mysqlDb.StructKeyColumn[r.Name()]["updated_at"] = &mysql.Field_struct{
					ColumnName: "updated_at",
				}
				db.mysqlDb.StructKeyColumn[r.Name()]["deleted_at"] = &mysql.Field_struct{
					ColumnName: "deleted_at",
				}
			} else {
				ColumnName := mysql.GetGormColumnName(field)
				db.mysqlDb.StructKeyColumn[r.Name()][field.Name] = &mysql.Field_struct{
					ColumnName: ColumnName,
					Offset:     field.Offset,
					Field_t:    field.Type,
				}
				db.mysqlDb.StructKeyColumn[r.Name()][ColumnName] = db.mysqlDb.StructKeyColumn[r.Name()][field.Name]
			}

		}
	}
}

//以下用mysqlDb实现
func (db *DBStruct) Create(i interface{}) *DBStruct {
	db = db.clone()
	r := reflect.ValueOf(i)
	if f := r.MethodByName("BeforeSave"); f.Kind() == reflect.Func {
		if err, ok := f.Call(nil)[0].Interface().(error); ok && err != nil {
			db.Error = err
		}
	}
	for r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	var id int64
	id, db.Error = db.build.Table(r.Type().Name()).Insert(i)
	r.FieldByName("Model").FieldByName("ID").SetUint(uint64(id))
	if f := reflect.ValueOf(i).MethodByName("AfterCreate"); db.Error == nil && f.Kind() == reflect.Func {
		if err, ok := f.Call([]reflect.Value{reflect.ValueOf(db.gormDb)})[0].Interface().(error); ok && err != nil {
			db.Error = err
		}
	}
	return db
}

func (db *DBStruct) Save(i interface{}) *DBStruct {
	db = db.clone()
	r := reflect.ValueOf(i)
	if f := r.MethodByName("BeforeSave"); f.Kind() == reflect.Func {
		if err, ok := f.Call(nil)[0].Interface().(error); ok && err != nil {
			db.Error = err
		}
	}
	for r.Kind() == reflect.Ptr {
		r = r.Elem()
	}

	field := r.FieldByName("Model")
	if field.Kind() == reflect.Invalid || field.Type().String() != "gorm.Model" {
		db.Error = errors.New("Save对象不包含gorm.Model")
	}
	if field.FieldByName("ID").Uint() == 0 {
		id, err := db.build.Table(r.Type().Name()).Insert(i)
		db.Error = err
		r.FieldByName("Model").FieldByName("ID").SetUint(uint64(id))
		if f := reflect.ValueOf(i).MethodByName("AfterCreate"); db.Error == nil && f.Kind() == reflect.Func {
			if err, ok := f.Call([]reflect.Value{reflect.ValueOf(db.gormDb)})[0].Interface().(error); ok && err != nil {
				db.Error = err
			}
		}
	} else {
		err := db.build.Table(r.Type().Name()).Replace(i)
		db.Error = err
	}
	return db
}
func (db *DBStruct) Where(i string, arg ...interface{}) *DBStruct {
	db = db.clone()
	db.build.Where(i, arg...)
	return db
}
func (db *DBStruct) Find(i interface{}) *DBStruct {
	db = db.clone()
	r := reflect.TypeOf(i)
	for r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	if r.Kind() == reflect.Slice {
		r = r.Elem()
		for r.Kind() == reflect.Ptr {
			r = r.Elem()
		}
	}
	db.Error = db.build.Table(r.Name()).Select(i)
	if db.build.Auto_preload {
		r := reflect.ValueOf(i)
		for r.Kind() == reflect.Ptr {
			r = r.Elem()
		}
		switch r.Kind() {
		case reflect.Slice:
			for i := 0; i < r.Len(); i++ {
				f := r.Index(i)
				for f.Kind() == reflect.Ptr {
					f = f.Elem()
				}
				if f.Kind() == reflect.Struct {
					db.getPreload(f)
				}
			}
		case reflect.Struct:
			db.getPreload(r)
		}
	}
	if f := reflect.ValueOf(i).MethodByName("AfterFind"); db.Error == nil && f.Kind() == reflect.Func {
		if err, ok := f.Call(nil)[0].Interface().(error); ok && err != nil {
			db.Error = err
		}
	}
	return db
}
func (db *DBStruct) Limit(i interface{}) *DBStruct {
	db = db.clone()
	var l uint64
	switch n := i.(type) {
	case int:
		l = uint64(n)
	case int8:
		l = uint64(n)
	case int16:
		l = uint64(n)
	case int32:
		l = uint64(n)
	case int64:
		l = uint64(n)
	case uint:
		l = uint64(n)
	case uint8:
		l = uint64(n)
	case uint16:
		l = uint64(n)
	case uint32:
		l = uint64(n)
	case uint64:
		l = n
	default:
		db.Error = errors.New("Limit 未设置类型" + reflect.TypeOf(i).Name())
	}
	db.build.Limit(l)
	return db
}
func (db *DBStruct) Offset(i interface{}) *DBStruct {
	db = db.clone()
	var l uint64
	switch n := i.(type) {
	case int:
		l = uint64(n)
	case int8:
		l = uint64(n)
	case int16:
		l = uint64(n)
	case int32:
		l = uint64(n)
	case int64:
		l = uint64(n)
	case uint:
		l = uint64(n)
	case uint8:
		l = uint64(n)
	case uint16:
		l = uint64(n)
	case uint32:
		l = uint64(n)
	case uint64:
		l = n
	default:
		db.Error = errors.New("Offset 未设置类型" + reflect.TypeOf(i).Name())
	}
	db.build.Offset(l)
	return db
}
func (db *DBStruct) Order(order string) *DBStruct {
	db = db.clone()
	db.build.Order(order)
	return db
}
func (db *DBStruct) First(i interface{}, id ...interface{}) *DBStruct {
	db = db.clone()
	r := reflect.TypeOf(i)
	for r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	if r.Kind() == reflect.Slice {
		r = r.Elem()
		for r.Kind() == reflect.Ptr {
			r = r.Elem()
		}
	}
	if len(id) == 1 {
		db.build.AddWhere("id = ?", id[0])
	}
	db = db.Limit(1).Find(i)
	if db.Error != nil {
		return db
	}

	return db
}
func (db *DBStruct) getPreload(r reflect.Value) {
	for i := 0; i < r.NumField(); i++ {
		f := r.Field(i)
		if f.Kind() == reflect.Struct && !strings.Contains(reflect.TypeOf(r.Interface()).Field(i).Tag.Get("gorm"), "PRELOAD:false") {
			id := r.FieldByName(f.Type().Name() + "ID")
			if id.Kind() == reflect.Uint {
				db := db.clone() //获取一个新的db
				db.First(f.Addr().Interface(), id.Uint())
			}

		}
	}
}
func (db *DBStruct) Model(i interface{}) *DBStruct {
	db = db.clone()
	r := reflect.ValueOf(i)
	for r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	if r.Kind() == reflect.Slice {
		r = r.Elem()
		for r.Kind() == reflect.Ptr {
			r = r.Elem()
		}
	}
	field := r.FieldByName("Model")
	if field.Kind() == reflect.Invalid || field.Type().String() != "gorm.Model" {
		db.Error = errors.New("Save对象不包含gorm.Model")
	}
	db.build.Table(r.Type().Name())
	if id := field.FieldByName("ID").Uint(); id > 0 {
		db.Where("id = ?", id)
	}

	return db
}
func (db *DBStruct) Delete(i interface{}) *DBStruct {
	db = db.clone()
	r := reflect.ValueOf(i)
	for r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	field := r.FieldByName("Model")
	if field.Kind() == reflect.Invalid || field.Type().String() != "gorm.Model" {
		db.Error = errors.New("Delete对象不包含gorm.Model")
	}
	if field.FieldByName("ID").Uint() == 0 {
		_, db.Error = db.build.Table(r.Type().Name()).Delete()
	} else {
		_, db.Error = db.build.Table(r.Type().Name()).Where("id = ?", field.FieldByName("ID").Uint()).Delete()
	}
	return db
}

func (db *DBStruct) Update(key string, value interface{}) *DBStruct {
	db = db.clone()
	db.build.Update(map[string]interface{}{key: value})
	return db
}
func (db *DBStruct) Set(name string, value interface{}) *DBStruct {
	db = db.clone()
	switch name {
	case "gorm:auto_preload":
		db.build.Auto_preload = value.(bool)
	case "gorm:association_autoupdate":
	default:
		fmt.Println("Set未设置", name)
	}
	return db
}

func (db *DBStruct) Updates(values map[string]interface{}) *DBStruct {
	db = db.clone()
	_, db.Error = db.build.Update(values)
	return db
}
func (db *DBStruct) UpdateColumn(key string, value interface{}) *DBStruct {
	return db.Update(key, value)
}

func (db *DBStruct) Count(c *int) *DBStruct {
	db = db.clone()
	*c, db.Error = db.build.Count()
	return db
}
func (db *DBStruct) Select(field string) *DBStruct {
	db = db.clone()
	db.build.Field(field)
	return db
}
func (db *DBStruct) Scan(dest interface{}) *DBStruct {
	db.Error = db.build.Scan(dest)
	return db
}
func (db *DBStruct) Row() *mysql.Mysql_Build {
	db = db.clone()
	return db.build
}
func (db *DBStruct) Reset() {
	db.slave = false
}
func (db *DBStruct) Or(i string, arg ...interface{}) *DBStruct {
	db = db.clone()
	db.build.AddWhereOr(i, arg...)
	return db
}
