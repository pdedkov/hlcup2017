package main

// TODO
// 1. encoding/json -> easyjson
// 2. sync.Pool
// 3. goroutine for post
// 4. sync.Mutex on structs

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
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// path to data folder
const zipPath = "/tmp/data/data.zip"

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
	ID        int    `json:"id"`
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
	ID       int    `json:"id"`
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
	ID       int    `json:"id"`
	User     int    `json:"user"`
	Location int    `json:"location"`
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
	Locations      map[int]Location
	Users          map[int]User
	Visits         map[int]Visit
	UserVisit      map[int]map[int]int
	LocationVisits map[int]map[int]int
}

// ValidateFilter validates passed filters
func (d Database) ParseFilters(args *fasthttp.Args) (map[string]interface{}, error) {
	conditions := make(map[string]interface{})

	if args.Has("fromDate") {
		v, err := strconv.Atoi(string(args.Peek("fromDate")))
		if err != nil {
			return nil, fmt.Errorf("Wrong from date %d", v)
		}
		conditions["fromDate"] = v
	}

	if args.Has("toDate") {
		v, err := strconv.Atoi(string(args.Peek("toDate")))
		if err != nil {
			return nil, fmt.Errorf("Wrong to date %d", v)
		}
		conditions["toDate"] = v
	}

	if args.Has("fromAge") {
		v, err := strconv.Atoi(string(args.Peek("fromAge")))
		if err != nil {
			return nil, fmt.Errorf("Wrong from age %d", v)
		}
		conditions["fromAge"] = v
	}

	if args.Has("toAge") {
		v, err := strconv.Atoi(string(args.Peek("toAge")))
		if err != nil {
			return nil, fmt.Errorf("Wrong to age %d", v)
		}
		conditions["toAge"] = v
	}

	if args.Has("gender") {
		v := string(args.Peek("gender"))
		if v != "m" && v != "f" {
			return nil, fmt.Errorf("Wrong gender %s", v)
		}
		conditions["gender"] = v
	}
	if args.Has("country") {
		v, _ := url.QueryUnescape(string(args.Peek("country")))
		conditions["country"] = v
	}

	if args.Has("toDistance") {
		v, err := strconv.Atoi(string(args.Peek("toDistance")))
		if err != nil {
			return nil, fmt.Errorf("Wrong distance %d", v)
		}
		conditions["toDistance"] = v
	}

	return conditions, nil
}

// Filter visits in database
func (d Database) FilterVisits(conditions map[string]interface{}, recs []Visit) []Visit {
	var out []Visit

	for _, rec := range recs {
		if v, ok := conditions["fromDate"]; ok {
			if rec.Visited < v.(int) {
				continue
			}
		}
		if v, ok := conditions["toDate"]; ok {
			if rec.Visited > v.(int) {
				continue
			}
		}
		if v, ok := conditions["fromAge"]; ok {
			if rec.Age < v.(int) {
				continue
			}
		}
		if v, ok := conditions["toAge"]; ok {
			if rec.Age >= v.(int) {
				continue
			}
		}
		if v, ok := conditions["toDistance"]; ok {
			if rec.Distance >= v.(int) {
				continue
			}
		}
		if v, ok := conditions["gender"]; ok {
			if rec.Gender != v.(string) {
				continue
			}
		}

		if v, ok := conditions["country"]; ok {
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
		return fmt.Errorf("Wrong file: %+v", err)
	}

	err = json.Unmarshal(b, v)
	if err != nil {
		return fmt.Errorf("Unable to unmarshal: %+v", err)
	}
	return nil
}

func calcAge(now int64, bd int64) int {
	diff := now - bd
	t := time.Unix(diff, 0)
	y, _, _ := t.Date()

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
	var (
		ls []Location
		vs []Visit
		us []User
	)
	var Db Database

	// prepare database
	// unzip
	err := archiver.Zip.Open(zipPath, dataPath)
	if err != nil {
		panic(err)
	}
	// load timestamp
	f, err := os.Open(dataPath + "options.txt")
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
				log.Printf("%s not exists", path)
				break
			}
			log.Printf("%s found. Parsing...", path)

			switch key {
			case "locations":
				var l Locations
				err := loadData(path, &l)
				if err != nil {
					panic(err)
				}
				log.Printf("Loaded %d locations", len(l.Records))
				ls = append(ls, l.Records...)
			case "users":
				var u Users
				err := loadData(path, &u)
				if err != nil {
					panic(err)
				}
				log.Printf("Loaded %d users", len(u.Records))
				us = append(us, u.Records...)
			case "visits":
				var v Visits
				err := loadData(path, &v)
				if err != nil {
					panic(err)
				}
				log.Printf("Loaded %d visits", len(v.Records))
				vs = append(vs, v.Records...)
			default:
				panic(fmt.Errorf("something went wrong"))
			}
		}
	}

	log.Print("Data loaded")

	// init maps
	Db.Locations = make(map[int]Location)
	for _, r := range ls {
		Db.Locations[r.ID] = r
	}
	log.Printf("Total %d locs", len(Db.Locations))

	Db.Users = make(map[int]User)
	for _, r := range us {
		Db.Users[r.ID] = r
	}
	log.Printf("Total %d users", len(Db.Users))

	Db.Visits = make(map[int]Visit)
	for _, r := range vs {
		r.Age = calcAge(NOW, Db.Users[r.User].Birthday)

		r.Gender = Db.Users[r.User].Gender

		r.Distance = Db.Locations[r.Location].Distance
		r.Country = Db.Locations[r.Location].Country

		Db.Visits[r.ID] = r
	}
	log.Printf("Total %d visits", len(Db.Visits))

	Db.UserVisit = make(map[int]map[int]int)
	Db.LocationVisits = make(map[int]map[int]int)

	for _, value := range Db.Visits {
		if _, ok := Db.UserVisit[value.User]; !ok {
			Db.UserVisit[value.User] = make(map[int]int)
		}
		Db.UserVisit[value.User][value.ID]++

		if _, ok := Db.LocationVisits[value.Location]; !ok {
			Db.LocationVisits[value.Location] = make(map[int]int)
		}
		Db.LocationVisits[value.Location][value.ID]++
	}
	log.Printf("Total %d user visits and %d loc visits", len(Db.UserVisit), len(Db.LocationVisits))
	log.Print("Data ready")

	router := fasthttprouter.New()

	router.GET("/users/:id", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))

		if err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}
		if _, ok := Db.Users[id]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		response, _ := Db.Users[id].MarshalJSON()
		OkResponse(c, response, false)
		return
	})

	router.GET("/visits/:id", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))

		if err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}
		if _, ok := Db.Visits[id]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		response, _ := Db.Visits[id].MarshalJSON()
		OkResponse(c, response, false)
		return
	})

	router.GET("/locations/:id", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))

		if err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}
		if _, ok := Db.Locations[id]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		response, _ := Db.Locations[id].MarshalJSON()
		OkResponse(c, response, false)
		return
	})

	router.GET("/users/:id/visits", func(c *fasthttp.RequestCtx) {
		id, err := strconv.Atoi(c.UserValue("id").(string))
		if err != nil {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		if _, ok := Db.Users[id]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}
		v := make([]ShortVisit, 0)
		filters, err := Db.ParseFilters(c.QueryArgs())
		if err != nil {
			ErrorResponse(c, fasthttp.StatusBadRequest, false)
			return
		}

		if _, ok := Db.UserVisit[id]; ok {
			var vs []Visit
			for vID, cnt := range Db.UserVisit[id] {
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

		if _, ok := Db.Locations[id]; !ok {
			ErrorResponse(c, fasthttp.StatusNotFound, false)
			return
		}

		filters, err := Db.ParseFilters(c.QueryArgs())
		if err != nil {
			ErrorResponse(c, fasthttp.StatusBadRequest, false)
			return
		}

		if _, ok := Db.LocationVisits[id]; ok {
			var vs []Visit
			for vID, cnt := range Db.LocationVisits[id] {
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
		var t RawUser
		if err := t.UnmarshalJSON(c.PostBody()); err != nil {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if string(t.ID) == "null" || string(t.Gender) == "null" || string(t.Birthday) == "null" || string(t.FirstName) == "null" || string(t.LastName) == "null" || string(t.Email) == "null" {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if c.UserValue("id").(string) != "new" {
			if _, ok := Db.Users[id]; !ok {
				ErrorResponse(c, fasthttp.StatusNotFound, true)
				return
			}
			u = Db.Users[id]
		} else {
			u = User{}
		}
		if len(t.ID) > 0 {
			u.ID, err = strconv.Atoi(string(t.ID))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
		}
		if c.UserValue("id").(string) != "new" && u.ID != id {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.FirstName) > 0 {
			f, _ := strconv.Unquote(string(t.FirstName))
			u.FirstName = strings.Trim(f, "\"")
		}
		if utf8.RuneCountInString(u.FirstName) > 50 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.LastName) > 0 {
			l, _ := strconv.Unquote(string(t.LastName))
			u.LastName = strings.Trim(l, "\"")
		}
		if utf8.RuneCountInString(u.LastName) > 50 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Gender) > 0 {
			g, _ := strconv.Unquote(string(t.Gender))
			u.Gender = strings.Trim(g, "\"")
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
			e, _ := strconv.Unquote(string(t.Email))
			u.Email = strings.Trim(e, "\"")
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
		var t RawVisit

		if err := t.UnmarshalJSON(c.PostBody()); err != nil {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if string(t.ID) == "null" || string(t.User) == "null" || string(t.Location) == "null" || string(t.Visited) == "null" || string(t.Mark) == "null" {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		var oldUser, oldLocation int

		if c.UserValue("id").(string) == "new" {
			v = Visit{}
		} else {
			if _, ok := Db.Visits[id]; !ok {
				ErrorResponse(c, fasthttp.StatusNotFound, true)
				return
			}
			v = Db.Visits[id]
		}
		if len(t.ID) > 0 {
			v.ID, err = strconv.Atoi(string(t.ID))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
		}
		if c.UserValue("id").(string) != "new" && v.ID != id {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}
		if len(t.User) > 0 {
			if v.User > 0 {
				oldUser = v.User
			}
			v.User, err = strconv.Atoi(string(t.User))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
		}

		if _, ok := Db.Users[v.User]; !ok {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Location) > 0 {
			if v.Location > 0 {
				oldLocation = v.Location
			}
			v.Location, err = strconv.Atoi(string(t.Location))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
		}
		if _, ok := Db.Locations[v.Location]; !ok {
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

		if _, ok := Db.UserVisit[oldUser][v.ID]; ok && oldUser > 0 {
			Db.UserVisit[oldUser][v.ID]--
		}

		if _, ok := Db.LocationVisits[oldLocation][v.ID]; ok && oldLocation > 0 {
			Db.LocationVisits[oldLocation][v.ID]--
		}

		if _, ok := Db.UserVisit[v.User]; !ok {
			Db.UserVisit[v.User] = make(map[int]int)
		}
		Db.UserVisit[v.User][v.ID]++

		if _, ok := Db.LocationVisits[v.Location]; !ok {
			Db.LocationVisits[v.Location] = make(map[int]int)
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
		var t RawLocation

		if err := t.UnmarshalJSON(c.PostBody()); err != nil {
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
			if _, ok := Db.Locations[id]; !ok {
				ErrorResponse(c, fasthttp.StatusNotFound, true)
				return
			}
			l = Db.Locations[id]
		}
		if len(t.ID) > 0 {
			l.ID, err = strconv.Atoi(string(t.ID))
			if err != nil {
				ErrorResponse(c, fasthttp.StatusBadRequest, true)
				return
			}
		}

		if c.UserValue("id").(string) != "new" && l.ID != id {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Country) > 0 {
			u, _ := strconv.Unquote(string(t.Country))
			l.Country = strings.Trim(u, "\"")
		}
		if utf8.RuneCountInString(l.Country) > 50 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.City) > 0 {
			u, _ := strconv.Unquote(string(t.City))
			l.City = strings.Trim(u, "\"")
		}
		if utf8.RuneCountInString(l.City) > 50 {
			ErrorResponse(c, fasthttp.StatusBadRequest, true)
			return
		}

		if len(t.Place) > 0 {
			u, _ := strconv.Unquote(string(t.Place))
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

	log.Print("Start server")
	// run server
	log.Fatal(fasthttp.ListenAndServe(port, router.Handler))
}
