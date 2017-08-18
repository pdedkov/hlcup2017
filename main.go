package main

import (
	"encoding/json"
	"fmt"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/standard"
	"github.com/mholt/archiver"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
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
type User struct {
	ID        int    `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
	Gender    string `json:"gender"`
	Birthday  int64  `json:"birth_date"`
}

// Users is an array of user
type Users struct {
	Records []User `json:"users"`
}

// Location struct
type Location struct {
	ID       int    `json:"id"`
	Distance int    `json:"distance"`
	Country  string `json:"country"`
	City     string `json:"city"`
	Place    string `json:"place"`
}

// Locations is an array of location
type Locations struct {
	Records []Location `json:"locations"`
}

// Visit struct contain user locations visits
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

// type Visits array of visit
type Visits struct {
	Records []Visit `json:"visits"`
}

type Database struct {
	Locations      map[int]Location
	Users          map[int]User
	Visits         map[int]Visit
	UserVisit      map[int]map[int]bool
	LocationVisits map[int]map[int]bool
}

// ValidateFilter validates passed filters
func (d Database) ValidateFilter(q url.Values) error {
	if len(q.Get("fromDate")) > 0 {
		v, err := strconv.Atoi(q.Get("fromDate"))
		if err != nil {
			return fmt.Errorf("Wrong from date %s", v)
		}
	}

	if len(q.Get("toDate")) > 0 {
		v, err := strconv.Atoi(q.Get("toDate"))
		if err != nil {
			return fmt.Errorf("Wrong to date %s", v)
		}
	}

	if len(q.Get("fromAge")) > 0 {
		v, err := strconv.Atoi(q.Get("fromAge"))
		if err != nil {
			return fmt.Errorf("Wrong from age %s", v)
		}
	}

	if len(q.Get("toAge")) > 0 {
		v, err := strconv.Atoi(q.Get("toAge"))
		if err != nil {
			return fmt.Errorf("Wrong to age %s", v)
		}
	}

	if len(q.Get("gender")) > 0 {
		v := q.Get("gender")
		if v != "m" && v != "f" {
			return fmt.Errorf("Wrong gender %s", v)
		}
	}

	if len(q.Get("toDistance")) > 0 {
		v, err := strconv.Atoi(q.Get("toDistance"))
		if err != nil {
			return fmt.Errorf("Wrong distance %s", v)
		}
	}
	return nil
}

// Filter visits in database
func (d Database) FilterVisits(q url.Values, recs []Visit) ([]Visit, error) {
	var fromDate, toDate, fromAge, toAge, toDistance int
	var gender, country string

	var err error
	conditions := make(map[string]int)

	if len(q.Get("fromDate")) > 0 {
		fromDate, err = strconv.Atoi(q.Get("fromDate"))
		if err != nil {
			return nil, fmt.Errorf("Wrong from date %s", fromDate)
		}
		conditions["fromDate"] = fromDate
	}

	if len(q.Get("toDate")) > 0 {
		toDate, err = strconv.Atoi(q.Get("toDate"))
		if err != nil {
			return nil, fmt.Errorf("Wrong to date %s", toDate)
		}
		conditions["toDate"] = toDate
	}

	if len(q.Get("fromAge")) > 0 {
		fromAge, err = strconv.Atoi(q.Get("fromAge"))
		if err != nil {
			return nil, fmt.Errorf("Wrong from age %s", fromAge)
		}
		conditions["fromAge"] = fromAge
	}

	if len(q.Get("toAge")) > 0 {
		toAge, err = strconv.Atoi(q.Get("toAge"))
		if err != nil {
			return nil, fmt.Errorf("Wrong to age %s", toAge)
		}
		conditions["toAge"] = toAge
	}

	if len(q.Get("gender")) > 0 {
		gender = q.Get("gender")
		if gender != "m" && gender != "f" {
			return nil, fmt.Errorf("Wrong gender %s", gender)
		}
	}

	if len(q.Get("country")) > 0 {
		country, _ = url.QueryUnescape(q.Get("country"))
	}

	if len(q.Get("toDistance")) > 0 {
		toDistance, err = strconv.Atoi(q.Get("toDistance"))
		if err != nil {
			return nil, fmt.Errorf("Wrong distance %s", toDistance)
		}
		conditions["toDistance"] = toDistance
	}

	var out []Visit
	for _, rec := range recs {
		if v, ok := conditions["fromDate"]; ok {
			if rec.Visited < v {
				continue
			}
		}
		if v, ok := conditions["toDate"]; ok {
			if rec.Visited > v {
				continue
			}
		}
		if v, ok := conditions["fromAge"]; ok {
			if rec.Age < v {
				continue
			}
		}
		if v, ok := conditions["toAge"]; ok {
			if rec.Age >= v {
				continue
			}
		}
		if v, ok := conditions["toDistance"]; ok {
			if rec.Distance >= v {
				continue
			}
		}

		if gender != "" {
			if rec.Gender != gender {
				continue
			}
		}

		if country != "" {
			if rec.Country != country {
				continue
			}
		}

		out = append(out, rec)
	}

	return out, nil
}

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

func main() {
	var (
		ls []Location
		vs []Visit
		us []User
	)
	var Db Database

	// prepare database
	go func() {
		// unzip
		err := archiver.Zip.Open(zipPath, dataPath)
		if err != nil {
			panic(err)
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
			bd := time.Unix(Db.Users[r.User].Birthday, 0)
			r.Age = int(time.Since(bd) / (time.Hour * 24 * 365))
			r.Gender = Db.Users[r.User].Gender

			r.Distance = Db.Locations[r.Location].Distance
			r.Country = Db.Locations[r.Location].Country

			Db.Visits[r.ID] = r
		}
		log.Printf("Total %d visits", len(Db.Visits))

		Db.UserVisit = make(map[int]map[int]bool)
		Db.LocationVisits = make(map[int]map[int]bool)

		for _, value := range Db.Visits {
			if _, ok := Db.UserVisit[value.User]; !ok {
				Db.UserVisit[value.User] = make(map[int]bool)
			}
			Db.UserVisit[value.User][value.ID] = true

			if _, ok := Db.LocationVisits[value.Location]; !ok {
				Db.LocationVisits[value.Location] = make(map[int]bool)
			}
			Db.LocationVisits[value.Location][value.ID] = true
		}
		log.Printf("Total %d user visits and %d loc visits", len(Db.UserVisit), len(Db.LocationVisits))
		log.Print("Data ready")
	}()

	e := echo.New()

	e.Get("/visits/:id", func(c echo.Context) error {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			return c.JSON(http.StatusNotFound, struct{}{})
		}

		if _, ok := Db.Visits[id]; !ok {
			return c.JSON(http.StatusNotFound, struct{}{})
		}
		return c.JSON(http.StatusOK, Db.Visits[id])
	})

	e.Post("/visits/:id", func(c echo.Context) error {
		id, err := strconv.Atoi(c.Param("id"))
		if c.Param("id") != "new" && err != nil {
			return c.JSON(http.StatusNotFound, struct{}{})
		}
		var v Visit
		var t struct {
			ID       json.RawMessage `json:"id"`
			User     json.RawMessage `json:"user"`
			Location json.RawMessage `json:"location"`
			Visited  json.RawMessage `json:"visited_at"`
			Mark     json.RawMessage `json:"mark"`
		}

		if err := c.Bind(&t); err != nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}
		if string(t.ID) == "null" || string(t.User) == "null" || string(t.Location) == "null" || string(t.Visited) == "null" || string(t.Mark) == "null" {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}
		mutex := &sync.RWMutex{}
		mutex.Lock()
		defer mutex.Unlock()

		var oldUser, oldLocation int

		if c.Param("id") == "new" {
			v = Visit{}
		} else {
			if _, ok := Db.Visits[id]; !ok {
				return c.JSON(http.StatusNotFound, struct{}{})
			}
			v = Db.Visits[id]
		}
		if len(t.ID) > 0 {
			v.ID, err = strconv.Atoi(string(t.ID))
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}
		}
		if c.Param("id") != "new" && v.ID != id {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}
		if len(t.User) > 0 {
			if v.User > 0 {
				oldUser = v.User
			}
			v.User, err = strconv.Atoi(string(t.User))
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}
		}

		if _, ok := Db.Users[v.User]; !ok {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.Location) > 0 {
			if v.Location > 0 {
				oldLocation = v.Location
			}
			v.Location, err = strconv.Atoi(string(t.Location))
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}
		}
		if _, ok := Db.Locations[v.Location]; !ok {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.Visited) > 0 {
			v.Visited, err = strconv.Atoi(string(t.Visited))
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}
		}
		if v.Visited < 946684800 || v.Visited > 1420156799 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.Mark) > 0 {
			v.Mark, err = strconv.Atoi(string(t.Mark))
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}
		}
		if v.Mark < 0 || v.Mark > 5 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if c.Param("id") == "new" {
			bd := time.Unix(Db.Users[v.User].Birthday, 0)
			v.Age = int(time.Since(bd) / (time.Hour * 24 * 365))
			v.Gender = Db.Users[v.User].Gender

			v.Distance = Db.Locations[v.Location].Distance
			v.Country = Db.Locations[v.Location].Country
		} else {
			if oldUser > 0 {
				delete(Db.UserVisit[v.User], oldUser)
			}

			if oldLocation > 0 {
				delete(Db.LocationVisits[v.Location], oldLocation)
			}
		}

		if _, ok := Db.UserVisit[v.User]; !ok {
			Db.UserVisit[v.User] = make(map[int]bool)
		}
		Db.UserVisit[v.User][v.ID] = true

		if _, ok := Db.LocationVisits[v.Location]; !ok {
			Db.LocationVisits[v.Location] = make(map[int]bool)
		}
		Db.LocationVisits[v.Location][v.ID] = true

		Db.Visits[v.ID] = v

		return c.JSON(http.StatusOK, struct{}{})
	})

	e.Post("/users/:id", func(c echo.Context) error {
		id, err := strconv.Atoi(c.Param("id"))
		if c.Param("id") != "new" && err != nil {
			return c.JSON(http.StatusNotFound, struct{}{})
		}
		var u User

		var t struct {
			ID        json.RawMessage `json:"id"`
			FirstName json.RawMessage `json:"first_name"`
			LastName  json.RawMessage `json:"last_name"`
			Email     json.RawMessage `json:"email"`
			Gender    json.RawMessage `json:"gender"`
			Birthday  json.RawMessage `json:"birth_date"`
		}

		if err := c.Bind(&t); err != nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if string(t.ID) == "null" || string(t.Gender) == "null" || string(t.Birthday) == "null" || string(t.FirstName) == "null" || string(t.LastName) == "null" || string(t.Email) == "null" {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		mutex := &sync.RWMutex{}
		mutex.Lock()
		defer mutex.Unlock()

		if c.Param("id") != "new" {
			if _, ok := Db.Users[id]; !ok {
				return c.JSON(http.StatusNotFound, struct{}{})
			}
			u = Db.Users[id]
		} else {
			u = User{}
		}
		if len(t.ID) > 0 {
			u.ID, err = strconv.Atoi(string(t.ID))
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}
		}
		if c.Param("id") != "new" && u.ID != id {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.FirstName) > 0 {
			f, _ := strconv.Unquote(string(t.FirstName))
			u.FirstName = strings.Trim(f, "\"")
		}
		if utf8.RuneCountInString(u.FirstName) > 50 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.LastName) > 0 {
			l, _ := strconv.Unquote(string(t.LastName))
			u.LastName = strings.Trim(l, "\"")
		}
		if utf8.RuneCountInString(u.LastName) > 50 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.Gender) > 0 {
			g, _ := strconv.Unquote(string(t.Gender))
			u.Gender = strings.Trim(g, "\"")
		}
		if u.Gender != "f" && u.Gender != "m" {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.Birthday) > 0 {
			u.Birthday, err = strconv.ParseInt(string(t.Birthday), 10, 64)
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}
		}

		if u.Birthday < -1262304000 || u.Birthday > 915235199 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.Email) > 0 {
			e, _ := strconv.Unquote(string(t.Email))
			u.Email = strings.Trim(e, "\"")
		}
		if utf8.RuneCountInString(u.Email) > 100 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		Db.Users[u.ID] = u

		return c.JSON(http.StatusOK, struct{}{})
	})

	e.Post("/locations/:id", func(c echo.Context) error {
		id, err := strconv.Atoi(c.Param("id"))
		if c.Param("id") != "new" && err != nil {
			return c.JSON(http.StatusNotFound, struct{}{})
		}
		var l Location
		var t struct {
			ID       json.RawMessage `json:"id"`
			Distance json.RawMessage `json:"distance"`
			Country  json.RawMessage `json:"country"`
			City     json.RawMessage `json:"city"`
			Place    json.RawMessage `json:"place"`
		}
		if err := c.Bind(&t); err != nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}
		if string(t.ID) == "null" || string(t.Distance) == "null" || string(t.Country) == "null" || string(t.City) == "null" || string(t.Place) == "null" {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		mutex := &sync.RWMutex{}
		mutex.Lock()
		defer mutex.Unlock()

		if c.Param("id") == "new" {
			l = Location{}
		} else {
			if _, ok := Db.Locations[id]; !ok {
				return c.JSON(http.StatusNotFound, struct{}{})
			}
			l = Db.Locations[id]
		}
		if len(t.ID) > 0 {
			l.ID, err = strconv.Atoi(string(t.ID))
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}
		}

		if c.Param("id") != "new" && l.ID != id {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.Country) > 0 {
			u, _ := strconv.Unquote(string(t.Country))
			l.Country = strings.Trim(u, "\"")
		}
		if utf8.RuneCountInString(l.Country) > 50 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.City) > 0 {
			u, _ := strconv.Unquote(string(t.City))
			l.City = strings.Trim(u, "\"")
		}
		if utf8.RuneCountInString(l.City) > 50 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if len(t.Place) > 0 {
			u, _ := strconv.Unquote(string(t.Place))
			l.Place = strings.Trim(u, "\"")
		}

		if len(t.Distance) > 0 {
			l.Distance, err = strconv.Atoi(string(t.Distance))
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}
		}

		Db.Locations[l.ID] = l

		return c.JSON(http.StatusOK, struct{}{})
	})

	e.Get("/locations/:id", func(c echo.Context) error {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			return c.JSON(http.StatusNotFound, struct{}{})
		}

		if _, ok := Db.Locations[id]; !ok {
			return c.JSON(http.StatusNotFound, struct{}{})
		}

		return c.JSON(http.StatusOK, Db.Locations[id])
	})

	e.Get("/users/:id", func(c echo.Context) error {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			return c.JSON(http.StatusNotFound, struct{}{})
		}

		if _, ok := Db.Users[id]; !ok {
			return c.JSON(http.StatusNotFound, struct{}{})
		}

		return c.JSON(http.StatusOK, Db.Users[id])
	})

	e.Get("/users/:id/visits", func(c echo.Context) error {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			return c.JSON(http.StatusNotFound, struct{}{})
		}

		if _, ok := Db.Users[id]; !ok {
			return c.JSON(http.StatusNotFound, struct{}{})
		}
		v := make([]ShortVisit, 0)

		if err := Db.ValidateFilter(c.QueryParams()); err != nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if _, ok := Db.UserVisit[id]; ok {
			// build visits array
			var vs []Visit
			for vID := range Db.UserVisit[id] {
				t := Db.Visits[vID]

				bd := time.Unix(Db.Users[t.User].Birthday, 0)
				t.Age = int(time.Since(bd) / (time.Hour * 24 * 365))
				t.Gender = Db.Users[t.User].Gender

				t.Distance = Db.Locations[t.Location].Distance
				t.Country = Db.Locations[t.Location].Country

				vs = append(vs, t)
			}
			recs, err := Db.FilterVisits(c.QueryParams(), vs)
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}

			for _, value := range recs {
				v = append(v, ShortVisit{
					value.Mark,
					value.Visited,
					Db.Locations[value.Location].Place,
				})
			}
			sort.Sort(ByVisited(v))
		}
		r := struct {
			Visits []ShortVisit `json:"visits"`
		}{v}

		response, _ := json.Marshal(r)
		c.Response().Header().Set("Content-Length", strconv.Itoa(len(response)))

		return c.JSONBlob(http.StatusOK, response)
	})

	e.Get("/locations/:id/avg", func(c echo.Context) error {
		var sum, count int
		var avg float64

		id, err := strconv.Atoi(c.Param("id"))
		if err != nil || id <= 0 {
			return c.JSON(http.StatusNotFound, struct{}{})
		}

		if _, ok := Db.Locations[id]; !ok {
			return c.JSON(http.StatusNotFound, struct{}{})
		}

		if err := Db.ValidateFilter(c.QueryParams()); err != nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if _, ok := Db.LocationVisits[id]; ok {
			var vs []Visit
			for vID := range Db.LocationVisits[id] {
				t := Db.Visits[vID]

				bd := time.Unix(Db.Users[t.User].Birthday, 0)
				t.Age = int(time.Since(bd) / (time.Hour * 24 * 365))
				t.Gender = Db.Users[t.User].Gender

				t.Distance = Db.Locations[t.Location].Distance
				t.Country = Db.Locations[t.Location].Country

				vs = append(vs, t)
			}
			recs, err := Db.FilterVisits(c.QueryParams(), vs)
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
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

		return c.JSON(http.StatusOK, struct {
			Avg float64 `json:"avg"`
		}{avg})
	})

	log.Print("Start server")
	// run server
	e.Run(standard.New(port))
}
