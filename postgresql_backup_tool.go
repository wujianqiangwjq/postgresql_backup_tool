package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/gohouse/gorose"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v2"
)

type ConfigItem struct {
	Host      string   `yaml:"host"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
	Port      string   `yaml:"port"`
	Database  string   `yaml:"database"`
	Driver    string   `yaml:"driver"`
	Prefix    string   `yaml:"prefix"`
	Dbtable   string   `yaml:"dbtable"`
	Field     []string `yaml:"field,flow"`
	Field_pri string   `yaml:"field_primary"`
}
type ConfigPro struct {
	Source ConfigItem `yaml:"source"`
	Dest   ConfigItem `yaml:"dest"`
}

func (cp *ConfigPro) GetConfig(path string) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalln(err)
	}
	err = yaml.Unmarshal(content, cp)
	if err != nil {
		log.Fatalln(err)
	}

}

func (ci *ConfigItem) ToMap() map[string]string {
	config := make(map[string]string)
	if ci.Host == "" {
		log.Fatalln("host is unvalid")
	}
	if ci.Username == "" {
		log.Fatalln("username is unvalid")
	}
	if ci.Password == "" {
		log.Fatalln("password is unvalid")
	}
	if ci.Port == "" {
		log.Fatalln("port is unvalid")
	}
	if ci.Database == "" {
		log.Fatalln("database is unvalid")
	}
	if ci.Driver != "mysql" && ci.Driver != "postgres" {
		log.Fatalln("driver not in [mysql, postgres]")
	}
	if ci.Dbtable == "" {
		log.Fatalln("dbtable is unvalid")
	}
	if len(ci.Field) == 0 {
		log.Fatalln("field is unvalid")
	}
	config["host"] = ci.Host
	config["username"] = ci.Username
	config["password"] = ci.Password
	config["port"] = ci.Port
	config["database"] = ci.Database
	config["driver"] = ci.Driver
	config["prefix"] = ci.Prefix
	config["host"] = ci.Host
	config["protocol"] = "tcp"
	return config

}

var Dbconifg map[string]interface{}
var Source_db string
var Dest_db string
var Source_field []string
var Dest_field []string
var Source_field_primary string
var Dest_field_primary string

func (cp *ConfigPro) ToMap() map[string]map[string]string {
	config := make(map[string]map[string]string)

	config["source"] = cp.Source.ToMap()
	config["dest"] = cp.Dest.ToMap()
	return config
}

//check字符串a 是否在字符数组中
func find(a string, slice []string) bool {
	res := false
	for _, item := range slice {
		if item == a {
			res = true
			break
		}
	}
	return res
}

//从source数据同步到dest,需要的数据集合
func sync(source_item map[string]interface{}, source_fields, dest_fields []string) map[string]interface{} {
	res := make(map[string]interface{})
	for index, item := range source_fields {
		res[transfer_char(dest_fields[index])] = source_item[item]
	}
	return res
}

//字符串转义函数，因为对数据库操作时需要加双引号,特殊字段需要 如select "create", insert操作
func transfer_chars(data_source []string) []string {
	res := make([]string, 0, len(data_source))
	for _, item := range data_source {
		res = append(res, "\""+item+"\"")
	}
	return res
}
func transfer_char(data_source string) string {
	return "\"" + data_source + "\""
}

//获取查询字段的sql语句，主键不在sql时，会自动添加,主键没有配置，默认是id
func getSql(id string, fields []string) string {
	if find(id, fields) {
		fields = transfer_chars(fields)
		return strings.Join(fields, ",")
	} else {
		fields = transfer_chars(fields)
		return transfer_char(id) + "," + strings.Join(fields, ",")
	}
}

//同步时判断这条记录是否已经存在dest表中
func exist_dest_id(source_id interface{}, dest_primary string, dest_table gorose.Database) bool {

	ddata, err := dest_table.Fields(dest_primary).Where(dest_primary, source_id).First()
	if err == nil && len(ddata) > 0 {
		return true
	} else {
		return false
	}
}

var file_log *os.File
var fileer error

func init() {
	cpath := flag.String("c", "config.yml", "config path")
	log_path := flag.String("log", "sync.log", "log path")
	flag.Parse()
	file_log, fileer = os.OpenFile(*log_path, os.O_RDWR, 0644)
	if fileer != nil {
		file_log, _ = os.Create(*log_path)
	}
	log.SetOutput(file_log)
	config := new(ConfigPro)
	Dbconifg = make(map[string]interface{})
	config.GetConfig(*cpath)
	Source_db = transfer_char(config.Source.Dbtable)
	Dest_db = transfer_char(config.Dest.Dbtable)
	Source_field = config.Source.Field
	Dest_field = config.Dest.Field
	if config.Source.Field_pri == "" {
		Source_field_primary = "id"
	} else {
		Source_field_primary = config.Source.Field_pri
	}
	if config.Dest.Field_pri == "" {
		Dest_field_primary = "id"
	} else {
		Dest_field_primary = config.Dest.Field_pri
	}
	Dbconifg["Connections"] = config.ToMap()
	Dbconifg["Default"] = "source"
	Dbconifg["SetMaxOpenConns"] = 0
	Dbconifg["SetMaxIdleConns"] = -1

}

func main() {
	sourceconnection, serr := gorose.Open(Dbconifg)
	if serr != nil {
		log.Fatalln(serr)
	}
	defer sourceconnection.Close()
	sdb := sourceconnection.NewDB()
	Stable := sdb.Table(Source_db)
	destconnection, derr := gorose.Open(Dbconifg, "dest")
	if derr != nil {
		log.Fatalln(derr)
	}
	defer destconnection.Close()
	dbd := destconnection.NewDB()
	Dtable := dbd.Table(Dest_db)
	Stable.Fields(getSql(Source_field_primary, Source_field)).Chunk(
		1000,
		func(data []map[string]interface{}) {
			for _, item := range data {
				if exist_dest_id(item[Source_field_primary], Dest_field_primary, *Dtable) {
					log.Println(item[Source_field_primary], "exist")
				} else {
					insertdata := sync(item, Source_field, Dest_field)
					_, iser := Dtable.Data(insertdata).Insert()
					if iser != nil {
						log.Println(iser)
					}

				}
			}

		})
	//	defer file_log.Close()
}
