package mysql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/qkofy/log"
)

var logger *log.Logger

func init() {
	logger = log.New(&log.Config{Traceback: true})
}

func MakeBackQuote(s, sep string) string {
	tmp := strings.Split(func(s, sep string) string {
		if sep != " " {
			s = strings.Replace(strings.Trim(s, sep + " "), " ", "", -1)
		}

		return strings.Replace(s, sep + sep, sep, -1)
	}(s, sep), sep)

	for i := 0; i < len(tmp); i++ {
		tmp[i] = strings.Replace(tmp[i], ".", "`.`", -1)

		if !strings.HasPrefix(tmp[i], "`") {
			tmp[i] = "`" + tmp[i]
		}

		if strings.Contains(tmp[i], "``.") {
			tmp[i] = strings.Replace(tmp[i], "``.", "`.", -1)
		}

		if strings.Contains(tmp[i], ".``") {
			tmp[i] = strings.Replace(tmp[i], ".``", ".`", -1)
		}

		if !strings.HasSuffix(tmp[i], "`") {
			tmp[i] += "`"
		}

		if sep == " " && i == 0 {
			break
		}
	}

	if sep == "," {
		sep = ", "
	}

	return strings.Join(tmp, sep)
}

func ReplaceAll(s string, r ...[2]string) string {
	for i := 0; i < len(r); i++ {
		s = strings.Replace(s, r[i][0], r[i][1],-1)
	}

	return s
}

func ParseWhere(where interface{}, andor string, prm *[]interface{}) string {
	var whr []string

	bys := func(s []string, w *[]string, p *[]interface{}) {
		if len(s) > 2 {
			*p = append(*p, s[2])
			s[2] = "?"
			s[0] = MakeBackQuote(s[0], " ")
			*w = append(*w, strings.Join(s, " "))
		} else if len(s) > 1 {
			*p = append(*p, s[1])
			s[1] = "?"
			s[0] = MakeBackQuote(s[0], " ")
			*w = append(*w, strings.Join(s, " = "))
		} else {
			*w = append(*w, s[0])
		}
	}

	i2s := func(i []interface{}) []string {
		v := make([]string, len(i))

		for k, vv := range i {
			v[k] = vv.(string)
		}

		return v
	}

	byi := func(i []interface{}, w *[]string, p *[]interface{}) {
		if len(i) > 2 {
			*p = append(*p, i[2])
			i[2] = "?"
			i[0] = MakeBackQuote(i[0].(string), " ")
			*w = append(*w, strings.Join(i2s(i), " "))
		} else if len(i) > 1 {
			*p = append(*p, i[1])
			i[1] = "?"
			i[0] = MakeBackQuote(i[0].(string), " ")
			*w = append(*w, strings.Join(i2s(i), " = "))
		} else {
			*w = append(*w, i[0].(string))
		}
	}

	mbq := func(s string) string {
		reg, err := regexp.Compile("(?i)(^|and|or|\\(|\\.)\\s*([a-z0-9_]+)")

		if err != nil {
			logger.Fatal(err)
		}

		s = reg.ReplaceAllString(s, "$1 `$2`")

		return ReplaceAll(s, [2]string{". ", "."}, [2]string{"( ", "("})
	}

	switch where.(type) {
	case string:
		whr = append(whr, mbq(where.(string)))
	case []string:
		bys(where.([]string), &whr, prm)
	case [][]string:
		for i := 0; i < len(where.([][]string)); i++ {
			bys(where.([][]string)[i], &whr, prm)
		}
	case []interface{}:
		for _, v := range where.([]interface{}) {
			switch v.(type) {
			case string:
				whr = append(whr, mbq(v.(string)))
			case []string:
				bys(v.([]string), &whr, prm)
			case []interface{}:
				byi(v.([]interface{}), &whr, prm)
			}
		}
	}

	return mbq(strings.TrimSpace(strings.Join(whr, andor)))
}

func MakeArgs(n int) []interface{} {
	args := make([]interface{}, n)

	for i := 0; i < n; i++ {
		args[i] = &args[i]
	}

	return args
}

func ItoS(i interface{}) string {
	switch i.(type) {
	case string:
		return i.(string)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", i)
	case []uint8:
		return string(i.([]uint8))
	case []string:
		return strings.Join(i.([]string), "")
	default:
		s := fmt.Sprintf("%v", i)

		if s == "<nil>" || s == "[]" || s == "map[]" {
			return ""
		}

		return s
	}
}

func IsInt(i interface{}) bool {
	switch i.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

func IsString(i interface{}) bool {
	switch i.(type) {
	case string:
		return true
	default:
		return false
	}
}

func MakeCharset(code string) string {
	switch code {
	case "binary":
		return "binary"
	case "gbk", "big5", "gb2312":
		return code + "_chinese_ci"
	case "latin5":
		return "latin5_turkish_ci"
	case "euckr":
		return "euckr_korean_ci"
	case "hp8":
		return "hp8_english_ci"
	case "tis620":
		return "tis620_thai_ci"
	case "dec8", "swe7":
		return code + "_swedish_ci"
	case "cp932", "eucjpms", "sjis", "ujis":
		return code + "_japanese_ci"
	default:
		return code + "_general_ci"
	}
}
