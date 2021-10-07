package mysql

import (
	"fmt"
	"testing"
)

var db = New(&Config{
	Database: "",
	Username: "",
	Password: "",
})

func TestMakeSQL(t *testing.T) {
	fmt.Println("table:",db.Table("users").table)
	fmt.Println("alias:",db.Alias("u").alias)
	fmt.Println("field.1:",db.Field("id, name, email").field)
	fmt.Println("field.2:",db.Field([]string{"id", "name", "phone"}).field)
	fmt.Println("force:",db.Force("idx_phone").force)
	fmt.Println("where.1:",db.Where("id > 0 and (phone = 12332 or phone = 32123) and status = 1").where)
	fmt.Println("where.2:",db.Where([]string{"status", "1"}).where)
	fmt.Println("where.3:",db.Where([]string{"name", "like", "%u%"}).where)
	fmt.Println("where.4:",db.Where([][]string{{"status", "1"},{"fail", ">", "5"}}).where)
	fmt.Println("where.5:",db.Where([]interface{}{"id = 1 or phone = 12211"}).where)
	fmt.Println("where.6:",db.Where([]interface{}{[]string{"status", "1"}, []string{"name", "like", "%u%"}}).where)
	fmt.Println("where.7:",db.Where([]interface{}{[]interface{}{"status", 1},[]interface{}{"fail", ">", 5}}).where)
	fmt.Println("where.8:",db.Where([]interface{}{"id = 1", "phone = 12211"}).where)
	fmt.Println("where.9:",db.Where([]interface{}{"id = 1", []string{"phone", "12211"}}).where)
	fmt.Println("where.0:",db.Where([]interface{}{"id = 1", []interface{}{"phone", 12211}}).where)
	fmt.Println("where:",db.Where([]interface{}{[]interface{}{"id", 1}, []string{"phone", "12211"}}).where)
	fmt.Println("where.11:",db.Where(map[string]interface{}{
		"and": "id > 0 and (phone = 12332 or phone = 32123) and status = 1",
	}).where)
	fmt.Println("where.12:",db.Where(map[string]interface{}{
		"and": []string{"status", "1"},
	}).where)
	fmt.Println("where.13:",db.Where(map[string]interface{}{
		"and": []string{"name", "like", "%u%"},
	}).where)
	fmt.Println("where.14:",db.Where(map[string]interface{}{
		"and": [][]string{{"status", "1"},{"fail", ">", "5"}},
	}).where)
	fmt.Println("where.15:",db.Where(map[string]interface{}{
		"and": []interface{}{"id = 1 or phone = 12211"},
	}).where)
	fmt.Println("where.16:",db.Where(map[string]interface{}{
		"and": []interface{}{[]string{"status", "1"}, []string{"name", "like", "%u%"}},
	}).where)
	fmt.Println("where.17:",db.Where(map[string]interface{}{
		"and": []interface{}{[]interface{}{"status", 1},[]interface{}{"fail", ">", 5}},
	}).where)
	fmt.Println("where.18:",db.Where(map[string]interface{}{
		"and": []interface{}{"id = 1", "phone = 12211"},
	}).where)
	fmt.Println("where.19:",db.Where(map[string]interface{}{
		"and": []interface{}{"id = 1", []string{"phone", "12211"}},
	}).where)
	fmt.Println("where.10:",db.Where(map[string]interface{}{
		"and": []interface{}{"id = 1", []interface{}{"phone", 12211}},
	}).where)
	fmt.Println("where.:",db.Where(map[string]interface{}{
		"and": []interface{}{[]interface{}{"id", 1}, []string{"phone", "12211"}},
	}).where)
	fmt.Println("where.21:",db.Where(map[string]interface{}{
		"or": "id > 0 and (phone = 12332 or phone = 32123) and status = 1",
	}).where)
	fmt.Println("where.22:",db.Where(map[string]interface{}{
		"or": []string{"status", "1"},
	}).where)
	fmt.Println("where.23:",db.Where(map[string]interface{}{
		"or": []string{"name", "like", "%u%"},
	}).where)
	fmt.Println("where.24:",db.Where(map[string]interface{}{
		"or": [][]string{{"status", "1"},{"fail", ">", "5"}},
	}).where)
	fmt.Println("where.25:",db.Where(map[string]interface{}{
		"or": []interface{}{"id = 1 or phone = 12211"},
	}).where)
	fmt.Println("where.26:",db.Where(map[string]interface{}{
		"or": []interface{}{[]string{"status", "1"}, []string{"name", "like", "%u%"}},
	}).where)
	fmt.Println("where.27:",db.Where(map[string]interface{}{
		"or": []interface{}{[]interface{}{"status", 1},[]interface{}{"fail", ">", 5}},
	}).where)
	fmt.Println("where.28:",db.Where(map[string]interface{}{
		"or": []interface{}{"id = 1", "phone = 12211"},
	}).where)
	fmt.Println("where.29:",db.Where(map[string]interface{}{
		"or": []interface{}{"id = 1", []string{"phone", "12211"}},
	}).where)
	fmt.Println("where.20:",db.Where(map[string]interface{}{
		"or": []interface{}{"id = 1", []interface{}{"phone", 12211}},
	}).where)
	fmt.Println("where..:",db.Where(map[string]interface{}{
		"or": []interface{}{[]interface{}{"id", 1}, []string{"phone", "12211"}},
	}).where)
	fmt.Println("where...:",db.Where(map[string]interface{}{
		"and": []interface{}{[]interface{}{"`u`.id", 1}, []string{"phone", "12211"}},
		"or": []interface{}{[]interface{}{"status", 1}, []string{"u.phone", "12211"}},
	}).where)
	fmt.Println("order.1:",db.Order("id desc, phone asc").order)
	fmt.Println("order.2:",db.Order([]string{"id desc", "phone asc"}).order)
	fmt.Println("limit.1:",db.Limit(1).limit)
	fmt.Println("limit.2:",db.Limit("2").limit)
	fmt.Println("limit.3:",db.Limit(0, 10).limit)
	fmt.Println("limit.4:",db.Limit(0, "10").limit)
	fmt.Println("limit.5:",db.Limit("0", 10).limit)
	fmt.Println("limit.6:",db.Limit("0", "10").limit)
	fmt.Println("query:",db.MakeSQL())
}

func TestSelect(t *testing.T) {
	res := db.Configure("Debug", true).Configure("Prefix", "pdf_").Table("admin").Select()

	for i := 0; i < len(res); i++ {
		for k, v := range res[i].(map[string]interface{}) {
			fmt.Println(k, ItoS(v))
		}

		fmt.Println()
	}
}

func TestFind(t *testing.T) {
	res := db.Configure("Debug", true).Configure("Prefix", "pdf_").Table("admin").Find()

	for k, v := range res {
		fmt.Println(k, ItoS(v))
	}
}

func TestValue(t *testing.T) {
	res := db.Configure("Debug", true).Configure("Prefix", "pdf_").Table("admin").Where("id = 1").Value("nickname")

	fmt.Println("nickname:",res)
}

func TestCount(t *testing.T) {
	res := db.Configure("Debug", true).Configure("Prefix", "pdf_").Table("admin").Count()

	fmt.Println("count:", res)
}

func TestQuery(t *testing.T) {
	res := db.Configure("Debug", true).Query("select * from pdf_admin")

	for i := 0; i < len(res); i++ {
		for k, v := range res[i].(map[string]interface{}) {
			fmt.Println(k, ItoS(v))
		}

		fmt.Println()
	}
}

func TestOneRow(t *testing.T) {
	res := db.Configure("Debug", true).OneRow("select * from pdf_admin limit 1")

	for k, v := range res {
		fmt.Println(k, ItoS(v))
	}
}

func TestExec(t *testing.T) {
	db.Configure("Debug", true).Exec("select * from pdf_admin limit 1")

	fmt.Println("lastId:", db.LastId)
	fmt.Println("rowNum:", db.RowNum)
}

func TestInsert(t *testing.T) {
	db.Configure("Debug", true).Table("pdf_hot").Insert(map[string]interface{}{
		"cid": 1,
		"name": "test",
		"url": "https://www.test.com",
	})

	fmt.Println("lastId:", db.LastId)
	fmt.Println("rowNum:", db.RowNum)
}

func TestUpdate(t *testing.T) {
	db.Configure("Debug", true).Table("pdf_hot").Where("id = 3").Update(map[string]interface{}{
		"cid": 2,
		"name": "ce shi",
		"url": "https://www.ceshi.com",
	})

	fmt.Println("lastId:", db.LastId)
	fmt.Println("rowNum:", db.RowNum)
}

func TestDelete(t *testing.T) {
	db.Configure("Debug", true).Table("pdf_hot").Where("id = 3").Delete()

	fmt.Println("lastId:", db.LastId)
	fmt.Println("rowNum:", db.RowNum)
}

func TestAutoIncrement(t *testing.T) {
	db.Configure("Debug", true).Table("pdf_hot").AutoIncrement(1)

	fmt.Println("lastId:", db.LastId)
	fmt.Println("rowNum:", db.RowNum)
}

func TestTruncate(t *testing.T) {
	db.Configure("Debug", true).Table("pdf_hot").Truncate()

	fmt.Println("lastId:", db.LastId)
	fmt.Println("rowNum:", db.RowNum)
}