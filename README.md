## 第一版优化内容：

 * webdav备注名 “只读”，这个密码下的webdav，只允许在“提交”目录里面增删改，其他目录均为只读功能
 * chan异步打印，util.Log()里面打印的内容不再加锁进行实时打印（减少1-2ms响应时间)
 * 增加models下user,webdav,floder缓存设计，减少mysql查询次数
 * 针对raidrive，拦截0kb的文件写入
