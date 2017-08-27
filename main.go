package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/mailru/easyjson"
	"github.com/mholt/archiver"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"io/ioutil"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// path to zip folder
const zipPath = "/tmp/data/"

// 3 files with data
const dataPath = "/root/data/"

//const dataPath = "/tmp/data/data/"

// port
const port = ":80"

//const port = ":8080"

var dataMap = map[string]string{
	"locations": "locations_%d.json",
	"users":     "users_%d.json",
	"visits":    "visits_%d.json",
}

// User type stuct
//easyjson:json
type User struct {
	ID        uint32 `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
	Gender    string `json:"gender"`
	Birthday  int64  `json:"birth_date"`
}

//easyjson:json
type RawUser struct {
	ID        easyjson.RawMessage `json:"id"`
	FirstName easyjson.RawMessage `json:"first_name"`
	LastName  easyjson.RawMessage `json:"last_name"`
	Email     easyjson.RawMessage `json:"email"`
	Gender    easyjson.RawMessage `json:"gender"`
	Birthday  easyjson.RawMessage `json:"birth_date"`
}

// Users is an array of user
type Users struct {
	Records []User `json:"users"`
}

// Location struct
//easyjson:json
type Location struct {
	ID       uint32 `json:"id"`
	Distance int    `json:"distance"`
	Country  string `json:"country"`
	City     string `json:"city"`
	Place    string `json:"place"`
}

//easyjson:json
type RawLocation struct {
	ID       easyjson.RawMessage `json:"id"`
	Distance easyjson.RawMessage `json:"distance"`
	Country  easyjson.RawMessage `json:"country"`
	City     easyjson.RawMessage `json:"city"`
	Place    easyjson.RawMessage `json:"place"`
}

// Locations is an array of location
type Locations struct {
	Records []Location `json:"locations"`
}

// Visit struct contain user locations visits
//easyjson:json
type Visit struct {
	ID       uint32 `json:"id"`
	User     uint32 `json:"user"`
	Location uint32 `json:"location"`
	Visited  int    `json:"visited_at"`
	Mark     int    `json:"mark"`
	Age      int    `json:"-"`
	Gender   string `json:"-"`
	Country  string `json:"-"`
	Distance int    `json:"-"`
}

//easyjson:json
type RawVisit struct {
	ID       easyjson.RawMessage `json:"id"`
	User     easyjson.RawMessage `json:"user"`
	Location easyjson.RawMessage `json:"location"`
	Visited  easyjson.RawMessage `json:"visited_at"`
	Mark     easyjson.RawMessage `json:"mark"`
}

// type Visits array of visit
type Visits struct {
	Records []Visit `json:"visits"`
}

type Database struct {
	Locations      map[uint32]Location
	Users          map[uint32]User
	Visits         map[uint32]Visit
	UserVisit      map[uint32]map[uint32]uint32
	LocationVisits map[uint32]map[uint32]uint32
}

// ValidateFilter validates passed filters
func (d Database) ParseFilters(args *fasthttp.Args) (map[string]interface{}, error) {
	conditions := make(map[string]interface{})

	var err error
	var v int
	if args.Has("fromDate") {
		v, err = strconv.Atoi(string(args.Peek("fromDate")))
		if err != nil {
			return nil, err
		}
		conditions["fromDate"] = v
	}

	if args.Has("toDate") {
		v, err = strconv.Atoi(string(args.Peek("toDate")))
		if err != nil {
			return nil, err
		}
		conditions["toDate"] = v
	}

	if args.Has("fromAge") {
		v, err = strconv.Atoi(string(args.Peek("fromAge")))
		if err != nil {
			return nil, err
		}
		conditions["fromAge"] = v
	}

	if args.Has("toAge") {
		v, err = strconv.Atoi(string(args.Peek("toAge")))
		if err != nil {
			return nil, err
		}
		conditions["toAge"] = v
	}

	if args.Has("gender") {
		g := string(args.Peek("gender"))
		if g != "m" && g != "f" {
			return nil, fmt.Errorf("Gender fail")
		}
		conditions["gender"] = g
	}
	if args.Has("country") {
		c, _ := url.QueryUnescape(string(args.Peek("country")))
		conditions["country"] = c
	}

	if args.Has("toDistance") {
		v, err = strconv.Atoi(string(args.Peek("toDistance")))
		if err != nil {
			return nil, err
		}
		conditions["toDistance"] = v
	}

	return conditions, nil
}

// Filter visits in database
func (d Database) FilterVisits(conditions map[string]interface{}, recs []Visit) []Visit {
	var out []Visit

	var ok bool
	var v interface{}
	for _, rec := range recs {
		if v, ok = conditions["fromDate"]; ok {
			if rec.Visited < v.(int) {
				continue
			}
		}
		if v, ok = conditions["toDate"]; ok {
			if rec.Visited > v.(int) {
				continue
			}
		}
		if v, ok = conditions["fromAge"]; ok {
			if rec.Age < v.(int) {
				continue
			}
		}
		if v, ok = conditions["toAge"]; ok {
			if rec.Age >= v.(int) {
				continue
			}
		}
		if v, ok = conditions["toDistance"]; ok {
			if rec.Distance >= v.(int) {
				continue
			}
		}
		if v, ok = conditions["gender"]; ok {
			if rec.Gender != v.(string) {
				continue
			}
		}

		if v, ok = conditions["country"]; ok {
			if rec.Country != v.(string) {
				continue
			}
		}

		out = append(out, rec)
	}

	return out
}

//easyjson:json
type Avg struct {
	Avg float64 `json:"avg"`
}

//easyjson:json
type ShortVisits struct {
	Visits []ShortVisit `json:"visits"`
}

type ShortVisit struct {
	Mark    int    `json:"mark"`
	Visited int    `json:"visited_at"`
	Place   string `json:"place"`
}

// Sort function
type ByVisited []ShortVisit

func (v ByVisited) Len() int {
	return len(v)
}
func (v ByVisited) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}
func (v ByVisited) Less(i, j int) bool {
	return v[i].Visited < v[j].Visited
}

var NOW int64

func loadData(path string, v interface{}) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, v)
	if err != nil {
		return err
	}
	return nil
}

func calcAge(now int64, bd int64) int {
	y, _, _ := time.Unix(now-bd, 0).Date()

	return y - 1970
}

func ErrorResponse(c *fasthttp.RequestCtx, code int, close bool) {
	c.Response.Header.Set("Content-Type", "application/json")
	c.Response.SetStatusCode(code)
	c.Write([]byte(`{}`))
	if close {
		c.SetConnectionClose()
	}
}

func OkResponse(c *fasthttp.RequestCtx, body []byte, close bool) {
	c.Response.Header.Set("Content-Type", "application/json")
	c.Response.SetStatusCode(fasthttp.StatusOK)
	c.Write(body)
	if close {
		c.SetConnectionClose()
	}
}

func main() {
	var Db Database
	Db.Locations = make(map[uint32]Location)
	Db.Users = make(map[uint32]User)
	Db.Visits = make(map[uint32]Visit)
	Db.UserVisit = make(map[uint32]map[uint32]uint32)
	Db.LocationVisits = make(map[uint32]map[uint32]uint32)

	var m runtime.MemStats

	// prepare database
	// unzip
	err := archiver.Zip.Open(zipPath+"data.zip", dataPath)
	if err != nil {
		panic(err)
	}
	// load timestamp
	f, err := os.Open(zipPath + "options.txt")
	if err == nil {
		// Start reading from the file with a reader.
		reader := bufio.NewReader(f)
		line, err := reader.ReadString('\n')
		if err == nil {
			tm, err := strconv.Atoi(strings.TrimRight(line, "\n"))
			if err == nil {
				NOW = int64(tm)
				log.Printf("get timestamp from options.txt %d", NOW)
			}
		}
		f.Close()
	}
	if NOW == 0 {
		log.Printf("open fail: %s", dataPath+"options.txt")
		info, err := os.Stat(zipPath)
		if err != nil {
			NOW = time.Now().Unix()
			log.Printf("get timestamp from time.Now() %d", NOW)
		} else {
			NOW = info.ModTime().Unix()
			log.Printf("get timestamp from mtime %d", NOW)
		}
	}

	// load data to structs
	for key, value := range dataMap {
		for i := 1; ; i++ {
			path := fmt.Sprintf(dataPath+value, i)
			if _, err := os.Stat(path); os.IsNotExist(err) {
				break
			}

			switch key {
			case "locations":
				var l Locations
				err = loadData(path, &l)
				if err != nil {
					panic(err)
				}
				log.Printf("Loaded %d locations", len(l.Records))
				for _, r := range l.Records {
					Db.Locations[r.ID] = r
				}
			case "users":
				var u Users
				err = loadData(path, &u)
				if err != nil {
					panic(err)
				}
				log.Printf("Loaded %d users", len(u.Records))
				for _, v := range u.Records {
					Db.Users[v.ID] = v
				}
			case "visits":
				var v Visits
				err = loadData(path, &v)
				if err != nil {
					panic(err)
				}
				log.Printf("Loaded %d visits", len(v.Records))
				for _, r := range v.Records {
					r.Age = calcAge(NOW, Db.Users[r.User].Birthday)

					r.Gender = Db.Users[r.User].Gender

					r.Distance = Db.Locations[r.Location].Distance
					r.Country = Db.Locations[r.Location].Country

					Db.Visits[r.ID] = r
				}
			default:
				panic(fmt.Errorf("something went wrong"))
			}
		}
	}
	log.Print("Data loaded")

	runtime.ReadMemStats(&m)
	log.Printf("Alloc=%v Sys=%v NumGC=%v", m.Alloc/1024, m.Sys/1024, m.NumGC)

	for _, value := range Db.Visits {
		if _, ok := Db.UserVisit[value.User]; !ok {
			Db.UserVisit[value.User] = make(map[uint32]uint32)
		}
		Db.UserVisit[value.User][value.ID]++

		if _, ok := Db.LocationVisits[value.Location]; !ok {
			Db.LocationVisits[value.Location] = make(map[uint32]uint32)
		}
		Db.LocationVisits[value.Location][value.ID]++
	}
	log.Print("Data ready")

	runtime.ReadMemStats(&m)
	log.Printf("Alloc=%v Sys=%v NumGC =%v", m.Alloc/1024, m.Sys/1024, m.NumGC)

	router := fasthttprouter.New()
	router.GET("/users/:id", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))

		if err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}
		if _, ok := Db.Users[uint32(id)]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		response, _ := Db.Users[uint32(id)].MarshalJSON()
		OkResponse(c, response, false)
		return
	})

	router.GET("/visits/:id", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))

		if err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}
		if _, ok := Db.Visits[uint32(id)]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		response, _ := Db.Visits[uint32(id)].MarshalJSON()
		OkResponse(c, response, false)
		return
	})

	router.GET("/locations/:id", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))

		if err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}
		if _, ok := Db.Locations[uint32(id)]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		response, _ := Db.Locations[uint32(id)].MarshalJSON()
		OkResponse(c, response, false)
		return
	})

	router.GET("/users/:id/visits", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))
		if err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		if _, ok := Db.Users[uint32(id)]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}
		v := make([]ShortVisit, 0)
		filters, err := Db.ParseFilters(c.QueryArgs())
		if err != nil {
			ErrorResponse(c, fasthttp.StatusBadRequest, false)
			return
		}

		if _, ok := Db.UserVisit[uint32(id)]; ok {
			var vs []Visit
			for vID, cnt := range Db.UserVisit[uint32(id)] {
				if cnt <= 0 {
					continue
				}

				t := Db.Visits[vID]

				t.Age = calcAge(NOW, Db.Users[t.User].Birthday)
				t.Gender = Db.Users[t.User].Gender

				t.Distance = Db.Locations[t.Location].Distance
				t.Country = Db.Locations[t.Location].Country

				vs = append(vs, t)
			}
			recs := Db.FilterVisits(filters, vs)

			for _, value := range recs {
				v = append(v, ShortVisit{
					value.Mark,
					value.Visited,
					Db.Locations[value.Location].Place,
				})
			}
			sort.Sort(ByVisited(v))
		}
		r := ShortVisits{v}

		response, _ := r.MarshalJSON()

		OkResponse(c, response, false)
		return
	})

	router.GET("/locations/:id/avg", func(c *fasthttp.RequestCtx) {
		var sum, count int
		var avg float64

		id, err := strconv.Atoi(c.UserValue("id").(string))
		if err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		if _, ok := Db.Locations[uint32(id)]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		filters, err := Db.ParseFilters(c.QueryArgs())
		if err != nil {
			ErrorResponse(c, fasthttp.StatusBadRequest, false)
			return
		}

		if _, ok := Db.LocationVisits[uint32(id)]; ok {
			var vs []Visit
			for vID, cnt := range Db.LocationVisits[uint32(id)] {
				if cnt <= 0 {
					continue
				}
				t := Db.Visits[vID]

				t.Age = calcAge(NOW, Db.Users[t.User].Birthday)
				t.Gender = Db.Users[t.User].Gender

				t.Distance = Db.Locations[t.Location].Distance
				t.Country = Db.Locations[t.Location].Country

				vs = append(vs, t)
			}
			recs := Db.FilterVisits(filters, vs)
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, false)
				return
			}

			for _, rec := range recs {
				sum += rec.Mark
				count++
			}

			if count > 0 {
				avg = float64(sum) / float64(count)
				tmp := int(avg * 100000)
				last := int(avg*1000000) - tmp*10
				if last >= 5 {
					tmp++
				}
				avg = float64(tmp) / 100000
			}
		}

		A := Avg{avg}
		response, _ := A.MarshalJSON()

		OkResponse(c, response, false)
		return
	})

	router.POST("/users/:id", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))

		if c.UserValue("id").(string) != "new" && err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, true)
			return
		}

		var u User
		var str string
		t := RawUser{}

		if err := t.UnmarshalJSON(c.PostBody()); err != nil {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if string(t.ID) == "null" || string(t.Gender) == "null" || string(t.Birthday) == "null" || string(t.FirstName) == "null" || string(t.LastName) == "null" || string(t.Email) == "null" {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if c.UserValue("id").(string) != "new" {
			if _, ok := Db.Users[uint32(id)]; !ok {
				ErrorResponse(c, fasthttp.StatusNotFound, true)
				return
			}
			u = Db.Users[uint32(id)]
		} else {
			u = User{}
		}
		if len(t.ID) > 0 {
			tId, err := strconv.Atoi(string(t.ID))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
			u.ID = uint32(tId)
		}
		if c.UserValue("id").(string) != "new" && u.ID != uint32(id) {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.FirstName) > 0 {
			str, _ = strconv.Unquote(string(t.FirstName))
			u.FirstName = strings.Trim(str, "\"")
		}
		if utf8.RuneCountInString(u.FirstName) > 50 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.LastName) > 0 {
			str, _ = strconv.Unquote(string(t.LastName))
			u.LastName = strings.Trim(str, "\"")
		}
		if utf8.RuneCountInString(u.LastName) > 50 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Gender) > 0 {
			str, _ = strconv.Unquote(string(t.Gender))
			u.Gender = strings.Trim(str, "\"")
		}
		if u.Gender != "f" && u.Gender != "m" {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Birthday) > 0 {
			u.Birthday, err = strconv.ParseInt(string(t.Birthday), 10, 64)
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
		}

		if u.Birthday < -1262304000 || u.Birthday > 915235199 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Email) > 0 {
			str, _ = strconv.Unquote(string(t.Email))
			u.Email = strings.Trim(str, "\"")
		}
		if utf8.RuneCountInString(u.Email) > 100 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}
		Db.Users[u.ID] = u

		OkResponse(c, []byte(`{}`), true)
		return
	})

	router.POST("/visits/:id", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))

		if c.UserValue("id").(string) != "new" && err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, true)
			return
		}

		var v Visit
		t := RawVisit{}

		if err = t.UnmarshalJSON(c.PostBody()); err != nil {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if string(t.ID) == "null" || string(t.User) == "null" || string(t.Location) == "null" || string(t.Visited) == "null" || string(t.Mark) == "null" {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		var oldUser, oldLocation uint32
		var ok bool

		if c.UserValue("id").(string) == "new" {
			v = Visit{}
		} else {
			if _, ok = Db.Visits[uint32(id)]; !ok {
				ErrorResponse(c, fasthttp.StatusNotFound, true)
				return
			}
			v = Db.Visits[uint32(id)]
		}
		if len(t.ID) > 0 {
			tId, err := strconv.Atoi(string(t.ID))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
			v.ID = uint32(tId)
		}
		if c.UserValue("id").(string) != "new" && v.ID != uint32(id) {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}
		if len(t.User) > 0 {
			if v.User > 0 {
				oldUser = v.User
			}
			tId, err := strconv.Atoi(string(t.User))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
			v.User = uint32(tId)
		}

		if _, ok = Db.Users[v.User]; !ok {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Location) > 0 {
			if v.Location > 0 {
				oldLocation = v.Location
			}
			tId, err := strconv.Atoi(string(t.Location))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
			v.Location = uint32(tId)
		}
		if _, ok = Db.Locations[v.Location]; !ok {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Visited) > 0 {
			v.Visited, err = strconv.Atoi(string(t.Visited))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
		}
		if v.Visited < 946684800 || v.Visited > 1420156799 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Mark) > 0 {
			v.Mark, err = strconv.Atoi(string(t.Mark))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
		}
		if v.Mark < 0 || v.Mark > 5 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if _, ok = Db.UserVisit[oldUser][v.ID]; ok && oldUser > 0 {
			Db.UserVisit[oldUser][v.ID]--
		}

		if _, ok = Db.LocationVisits[oldLocation][v.ID]; ok && oldLocation > 0 {
			Db.LocationVisits[oldLocation][v.ID]--
		}

		if _, ok = Db.UserVisit[v.User]; !ok {
			Db.UserVisit[v.User] = make(map[uint32]uint32)
		}
		Db.UserVisit[v.User][v.ID]++

		if _, ok = Db.LocationVisits[v.Location]; !ok {
			Db.LocationVisits[v.Location] = make(map[uint32]uint32)
		}
		Db.LocationVisits[v.Location][v.ID]++

		Db.Visits[v.ID] = v

		OkResponse(c, []byte(`{}`), true)
		return
	})

	router.POST("/locations/:id", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))

		if c.UserValue("id").(string) != "new" && err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, true)
			return
		}
		var l Location
		t := RawLocation{}
		var u string
		if err = t.UnmarshalJSON(c.PostBody()); err != nil {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}
		if string(t.ID) == "null" || string(t.Distance) == "null" || string(t.Country) == "null" || string(t.City) == "null" || string(t.Place) == "null" {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if c.UserValue("id").(string) == "new" {
			l = Location{}
		} else {
			if _, ok := Db.Locations[uint32(id)]; !ok {
				ErrorResponse(c, fasthttp.StatusNotFound, true)
				return
			}
			l = Db.Locations[uint32(id)]
		}
		if len(t.ID) > 0 {
			tId, err := strconv.Atoi(string(t.ID))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
			l.ID = uint32(tId)
		}

		if c.UserValue("id").(string) != "new" && l.ID != uint32(id) {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Country) > 0 {
			u, _ = strconv.Unquote(string(t.Country))
			l.Country = strings.Trim(u, "\"")
		}
		if utf8.RuneCountInString(l.Country) > 50 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.City) > 0 {
			u, _ = strconv.Unquote(string(t.City))
			l.City = strings.Trim(u, "\"")
		}
		if utf8.RuneCountInString(l.City) > 50 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Place) > 0 {
			u, _ = strconv.Unquote(string(t.Place))
			l.Place = strings.Trim(u, "\"")
		}

		if len(t.Distance) > 0 {
			l.Distance, err = strconv.Atoi(string(t.Distance))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
		}

		Db.Locations[l.ID] = l

		OkResponse(c, []byte(`{}`), true)
		return
	})
	log.Fatal(fasthttp.ListenAndServe(port, router.Handler))
}
