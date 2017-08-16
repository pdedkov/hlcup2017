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
	ID        *int    `json:"id"`
	FirstName *string `json:"first_name"`
	LastName  *string `json:"last_name"`
	Email     *string `json:"email"`
	Gender    *string `json:"gender"`
	Birthday  *int64  `json:"birth_date"`
}

// Users is an array of user
type Users struct {
	Records []User `json:"users"`
}

// Location struct
type Location struct {
	ID       *int    `json:"id"`
	Distance *int    `json:"distance"`
	Country  *string `json:"country"`
	City     *string `json:"city"`
	Place    *string `json:"place"`
}

// Locations is an array of location
type Locations struct {
	Records []Location `json:"locations"`
}

// Visit struct contain user locations visits
type Visit struct {
	ID       *int   `json:"id"`
	User     *int   `json:"user"`
	Location *int   `json:"location"`
	Visited  *int   `json:"visited_at"`
	Mark     *int   `json:"mark"`
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
	UserVisit      map[int][]Visit
	LocationVisits map[int][]Visit
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
			if *rec.Visited < v {
				continue
			}
		}
		if v, ok := conditions["toDate"]; ok {
			if *rec.Visited > v {
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
			Db.Locations[*r.ID] = r
		}
		log.Printf("Total %d locs", len(Db.Locations))

		Db.Users = make(map[int]User)
		for _, r := range us {
			Db.Users[*r.ID] = r
		}
		log.Printf("Total %d users", len(Db.Users))

		Db.Visits = make(map[int]Visit)
		for _, r := range vs {
			bd := time.Unix(*Db.Users[*r.User].Birthday, 0)
			r.Age = int(time.Since(bd) / (time.Hour * 24 * 365))
			r.Gender = *Db.Users[*r.User].Gender

			r.Distance = *Db.Locations[*r.Location].Distance
			r.Country = *Db.Locations[*r.Location].Country

			Db.Visits[*r.ID] = r
		}
		log.Printf("Total %d visits", len(Db.Visits))

		Db.UserVisit = make(map[int][]Visit)
		Db.LocationVisits = make(map[int][]Visit)

		for _, value := range Db.Visits {
			Db.UserVisit[*value.User] = append(Db.UserVisit[*value.User], value)

			Db.LocationVisits[*value.Location] = append(Db.LocationVisits[*value.Location], value)
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
		if c.Param("id") == "new" {
			v = Visit{}
		} else {
			if _, ok := Db.Visits[id]; !ok {
				return c.JSON(http.StatusNotFound, struct{}{})
			}
			v = Db.Visits[id]
		}
		if err := c.Bind(&v); err != nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if v.ID == nil || v.User == nil || v.Location == nil || v.Visited == nil || v.Mark == nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if c.Param("id") != "new" && *v.ID != id {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if _, ok := Db.Users[*v.User]; !ok {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if _, ok := Db.Locations[*v.Location]; !ok {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if *v.Visited < 946684800 || *v.Visited > 1420156799 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if *v.Mark < 0 || *v.Mark > 5 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		mutex := &sync.RWMutex{}
		mutex.Lock()
		bd := time.Unix(*Db.Users[*v.User].Birthday, 0)
		v.Age = int(time.Since(bd) / (time.Hour * 24 * 365))
		v.Gender = *Db.Users[*v.User].Gender

		v.Distance = *Db.Locations[*v.Location].Distance
		v.Country = *Db.Locations[*v.Location].Country

		Db.UserVisit[*v.User] = append(Db.UserVisit[*v.User], v)
		Db.LocationVisits[*v.Location] = append(Db.LocationVisits[*v.Location], v)

		Db.Visits[*v.ID] = v
		mutex.Unlock()

		return c.JSON(http.StatusOK, struct{}{})
	})

	e.Post("/users/:id", func(c echo.Context) error {
		id, err := strconv.Atoi(c.Param("id"))
		if c.Param("id") != "new" && err != nil {
			return c.JSON(http.StatusNotFound, struct{}{})
		}
		var u User
		if c.Param("id") == "new" {
			u = User{}
		} else {
			if _, ok := Db.Users[id]; !ok {
				return c.JSON(http.StatusNotFound, struct{}{})
			}
			u = Db.Users[id]
		}

		if err := c.Bind(&u); err != nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if u.ID == nil || u.Gender == nil || u.Birthday == nil || u.FirstName == nil || u.LastName == nil || u.Email == nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if c.Param("id") != "new" && *u.ID != id {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if *u.Gender != "f" && *u.Gender != "m" {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if *u.Birthday < -1262304000 || *u.Birthday > 915235199 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if utf8.RuneCountInString(*u.FirstName) > 50 || utf8.RuneCountInString(*u.FirstName) <= 0 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if utf8.RuneCountInString(*u.LastName) > 50 || utf8.RuneCountInString(*u.LastName) <= 0 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if utf8.RuneCountInString(*u.Email) > 100 || utf8.RuneCountInString(*u.Email) <= 0 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		mutex := &sync.RWMutex{}
		mutex.Lock()
		Db.Users[*u.ID] = u
		mutex.Unlock()

		return c.JSON(http.StatusOK, struct{}{})
	})

	e.Post("/locations/:id", func(c echo.Context) error {
		id, err := strconv.Atoi(c.Param("id"))
		if c.Param("id") != "new" && err != nil {
			return c.JSON(http.StatusNotFound, struct{}{})
		}
		var l Location
		if c.Param("id") == "new" {
			l = Location{}
		} else {
			if _, ok := Db.Locations[id]; !ok {
				return c.JSON(http.StatusNotFound, struct{}{})
			}
			l = Db.Locations[id]
		}

		if err := c.Bind(&l); err != nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if l.ID == nil || l.Distance == nil || l.Country == nil || l.City == nil || l.Place == nil {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if c.Param("id") != "new" && *l.ID != id {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if utf8.RuneCountInString(*l.Country) > 50 || utf8.RuneCountInString(*l.Country) <= 0 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		if utf8.RuneCountInString(*l.City) > 50 || utf8.RuneCountInString(*l.City) <= 0 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}
		if utf8.RuneCountInString(*l.Place) <= 0 {
			return c.JSON(http.StatusBadRequest, struct{}{})
		}

		mutex := &sync.RWMutex{}
		mutex.Lock()
		Db.Locations[*l.ID] = l
		mutex.Unlock()

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

		if _, ok := Db.UserVisit[id]; ok {
			recs, err := Db.FilterVisits(c.QueryParams(), Db.UserVisit[id])
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}

			for _, value := range recs {
				v = append(v, ShortVisit{
					*value.Mark,
					*value.Visited,
					*Db.Locations[*value.Location].Place,
				})
			}
			sort.Sort(ByVisited(v))
		}

		return c.JSON(http.StatusOK, struct {
			Visits []ShortVisit `json:"visits"`
		}{v})
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

		if _, ok := Db.LocationVisits[id]; ok {
			recs, err := Db.FilterVisits(c.QueryParams(), Db.LocationVisits[id])
			if err != nil {
				return c.JSON(http.StatusBadRequest, struct{}{})
			}

			for _, rec := range recs {
				sum += *rec.Mark
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
