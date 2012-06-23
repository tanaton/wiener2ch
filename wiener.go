package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"strconv"
	"time"
	"github.com/tanaton/get2ch-go"
	"code.google.com/p/go-charset/charset"
	_ "code.google.com/p/go-charset/data"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/thrsafe"
)

type Nich struct {
	server		string
	board		string
	thread		string
}

type DataBase struct {
	host		string
	port		int
	user		string
	pass		string
	name		string
}

type SalamiConfig struct {
	host		string
	port		int
}

type Section struct {
	sc			SalamiConfig
	slch		<-chan map[string]string
	bl			[]string
	sl			map[string][]Nich
	bbn			bool
	bbnTime		time.Duration
}

type Config struct {
	v			map[string]interface{}
	section		[]*Section
	db			*DataBase
}

type Session struct {
	db			mysql.Conn
	get			*get2ch.Get2ch
}

type Packet struct {
	key			string
	jmp			bool
}

const (
	GO_THREAD_SLEEP_TIME	= 2 * time.Second
	THREAD_SLEEP_TIME		= 4 * time.Second
	RESTART_SLEEP_TIME		= 60 * time.Second
	CONFIG_JSON_PATH_DEF	= "wiener.json"
)

var g_reg_bbs *regexp.Regexp = regexp.MustCompile("(.+\\.2ch\\.net|.+\\.bbspink\\.com)/(.+)<>")
var g_reg_dat *regexp.Regexp = regexp.MustCompile("^(.+)\\.dat<>")
var g_reg_res *regexp.Regexp = regexp.MustCompile(" [(]([0-9]+)[)]$")
var g_reg_date *regexp.Regexp = regexp.MustCompile("^.*?<>.*?<>.*?(\\d{4})\\/(\\d{2})\\/(\\d{2}).*?(\\d{2}):(\\d{2}):(\\d{2})")
var g_cache get2ch.Cache = get2ch.NewFileCache("/2ch/dat")

var g_filter map[string]bool = map[string]bool{
	"ipv6.2ch.net"			: true,
	"headline.2ch.net"		: true,
	"headline.bbspink.com"	: true,
}

func main() {
	c := readConfig()
	loadSectionList(c)
	sync := make(chan *Section)
	// メイン処理
	for _, sec := range c.section {
		go sec.mainSection(c, sync)
		time.Sleep(GO_THREAD_SLEEP_TIME)
	}
	for {
		// 処理を止める
		sec := <-sync
		go sec.mainSection(c, sync)
	}
}

func (sec *Section) mainSection(c *Config, sync chan *Section) {
	var db mysql.Conn
	pch := make(chan *Packet, 2)
	fch := make(chan bool)

	if c.db != nil {
		db = mysql.New("tcp", "", fmt.Sprintf("%s:%d", c.db.host, c.db.port), c.db.user, c.db.pass, c.db.name)
		err := db.Connect()
		if err != nil { db = nil }
	}

	for key := range sec.sl {
		go sec.mainThread(key, db, pch, fch)
		time.Sleep(GO_THREAD_SLEEP_TIME)
	}
	for {
		pack := <-pch
		if pack == nil || pack.jmp {
			// 鯖移転の可能性
			close(fch)
			break
		} else if checkOpen(fch) {
			// 移転なし
			go sec.mainThread(pack.key, db, pch, fch)
		}
		time.Sleep(GO_THREAD_SLEEP_TIME)
	}
	time.Sleep(RESTART_SLEEP_TIME)
	close(pch)
	sync <- sec
}

func (sec *Section) mainThread(key string, db mysql.Conn, pch chan *Packet, fch <-chan bool) {
	jmp := false
	ses := &Session{
		get		: get2ch.NewGet2ch(get2ch.NewFileCache("/2ch/dat")),
	}
	if sec.sc.host != "" {
		ses.get.SetSalami(sec.sc.host, sec.sc.port)
	}
	ses.db = db

	bl, ok := sec.sl[key];
	if ok == false { return }

	for _, nich := range bl {
		if !checkOpen(fch) { return }
		// 板の取得
		tl, err := ses.getBoard(nich)
		if err == nil {
			if tl != nil && len(tl) > 0 {
				bid := ses.getMysqlBoardNo(nich.board)
				// スレッドの取得
				if !ses.getThread(tl, bid, fch) {
					return
				}
			}
		} else {
			// 板が移転した可能性あり
			jmp = true
		}
		// 止める
		time.Sleep(THREAD_SLEEP_TIME)
	}
	if checkOpen(fch) {
		pch <- &Packet{
			key		: key,
			jmp		: jmp,
		}
	}
}

func checkOpen(ch <-chan bool) bool {
	select {
	case <-ch:
		// chanがクローズされると即座にゼロ値が返ることを利用
		return false
	default:
		break
	}
	return true
}

func (ses *Session) getBoard(nich Nich) ([]Nich, error) {
	err := ses.get.SetRequest(nich.server, nich.board, "")
	if err != nil { return nil, err }
	h := threadResList(nich, ses.get.Cache)
	data, err := ses.get.GetData()
	if err != nil {
		log.Printf(err.Error() + "\n")
		return nil, err
	}
	ses.get.GetBoardName()
	vect := make([]Nich, 0, 1)
	list := strings.Split(string(data), "\n")
	for _, it := range list {
		if da := g_reg_dat.FindStringSubmatch(it); len(da) > 0 {
			if d := g_reg_res.FindStringSubmatch(it); len(d) > 0 {
				var n Nich
				n.server = nich.server
				n.board = nich.board
				n.thread = da[1]
				if m, ok := h[da[1]]; ok {
					if j, err := strconv.Atoi(d[1]); err == nil && m != j {
						vect = append(vect, n)
					}
				} else {
					vect = append(vect, n)
				}
			}
		}
	}
	return vect, nil
}

func (ses *Session) getThread(tl []Nich, bid int, fch <-chan bool) bool {
	for _, nich := range tl {
		if !checkOpen(fch) { return false }
		err := ses.get.SetRequest(nich.server, nich.board, nich.thread)
		if err != nil { continue }
		moto, err := ses.get.Cache.GetData(nich.server, nich.board, nich.thread)
		if err != nil { moto = nil }
		data, err := ses.get.GetData()
		if err != nil {
			log.Printf(err.Error() + "\n")
			log.Printf("%s/%s/%s\n", nich.server, nich.board, nich.thread)
		} else {
			log.Printf("%d OK %s/%s/%s\n", ses.get.Info.GetCode(), nich.server, nich.board, nich.thread)
			if bid >= 0 && ses.db != nil {
				ses.setMysqlRes(data, moto, nich, bid)
			}
		}
		// 4秒止める
		time.Sleep(THREAD_SLEEP_TIME)
	}
	return true
}

func (ses *Session) getMysqlBoardNo(bname string) (bid int) {
	var err error
	bid = -1

	if ses.db != nil {
		bid, err = ses.getMysqlBoardNoQuery(bname)
		if err != nil {
			err = ses.setMysqlBoardNoQuery(bname)
			if err != nil { return }
			bid, err = ses.getMysqlBoardNoQuery(bname)
		}
	}
	return
}

func (ses *Session) getMysqlBoardNoQuery(bname string) (int, error) {
	bid := -1
	rows, _, err := ses.db.Query("SELECT id FROM boardlist WHERE board = '%s'", bname)
	if err != nil { return -1, err }

	for _, row := range rows {
		bid = row.Int(0)
		break
	}
	if bid < 0 { return -1, errors.New("board id error") }
	return bid, nil
}

func (ses *Session) setMysqlBoardNoQuery(bname string) error {
	_, _, err := ses.db.Query("INSERT INTO boardlist (board) VALUES('%s')", bname)
	return err
}

func (ses *Session) getMysqlThreadNo(data []byte, bid int, tstr string) (tid int) {
	var err error
	tid = -1

	if ses.db != nil {
		tid, err = ses.getMysqlThreadNoQuery(bid, tstr)
		if err != nil {
			index := bytes.IndexByte(data, '\n')
			bl := bytes.Split(data[0:index], []byte{'<','>'})
			if len(bl) <= 4 { return }
			var title string
			title, err = sjisToUtf8(bl[4])
			if err != nil { return }
			err = ses.setMysqlThreadNoQuery(bid, tstr, title)
			if err != nil { return }
			tid, err = ses.getMysqlThreadNoQuery(bid, tstr)
		}
	}
	return
}

func (ses *Session) getMysqlThreadNoQuery(bid int, tstr string) (int, error) {
	tid := -1
	rows, _, err := ses.db.Query("SELECT id FROM threadlist WHERE board_id=%d AND thread=%d", bid, tstr)
	if err != nil { return -1, err }

	for _, row := range rows {
		tid = row.Int(0)
		break
	}
	if tid < 0 { return -1, errors.New("board id error") }
	return tid, nil
}

func (ses *Session) setMysqlThreadNoQuery(bid int, tstr, title string) error {
	_, _, err := ses.db.Query("INSERT INTO threadlist (board_id, thread, title) VALUES(%d,%s,'%s')", bid, tstr, ses.db.EscapeString(title))
	return err
}


func (ses *Session) setMysqlRes(data, moto []byte, n Nich, bid int) {
	if data == nil { return }
	ses.getMysqlThreadNo(data, bid, n.thread)
	if moto == nil {
		ses.setMysqlResQuery(data, n, bid, 0)
	} else {
		mlen := len(moto)
		if len(data) > (mlen + 1) {
			resno := bytes.Count(moto, []byte{'\n'})
			ses.setMysqlResQuery(data[mlen:], n, bid, resno)
		}
	}
}

func (ses *Session) setMysqlResQuery(data []byte, n Nich, bid, resno int) {
	str, err := sjisToUtf8(data)
	if err != nil { return }

	ql := make([]string, 0, 1)
	query :="INSERT INTO restime (thread_id,number,date) VALUES"
	list := strings.Split(str, "\n")
	for _, it := range list {
		resno++
		q, err := ses.createDateQuery(n, bid, resno, it)
		if err == nil {
			ql = append(ql, q)
		}
	}
	l := len(ql)
	if l > 0 {
		query += strings.Join(ql, ",")
		_, _, err := ses.db.Query(query)
		if err != nil {
			fmt.Printf("error: mysql挿入失敗\n");
		} else {
			fmt.Printf("mysql挿入数:%d\n", l);
		}
	}
}

func (ses *Session) createDateQuery(n Nich, bid, resno int, line string) (str string, err error) {
	if d := g_reg_date.FindStringSubmatch(line); len(d) > 6 {
		str = fmt.Sprintf("(%d,%s,%d,'%s%s%s%s%s%s')", bid, n.thread, resno, d[1], d[2], d[3], d[4], d[5], d[6])
	} else {
		err = errors.New("reg error")
	}
	return
}

func sjisToUtf8(data []byte) (string, error) {
	r, err := charset.NewReader("cp932", bytes.NewReader(data))
	if err != nil { return "", err }
	result, err := ioutil.ReadAll(r)
	return string(result), err
}

func getServer() map[string]string {
	get := get2ch.NewGet2ch(get2ch.NewFileCache("/2ch/dat"))
	bl := make(map[string]string)
	data, err := get.GetServer()
	if err != nil {
		data, err = get.Cache.GetData("", "", "")
		if err != nil { panic(err.Error()) }
	}
	list := strings.Split(string(data), "\n")
	for _, it := range list {
		if d := g_reg_bbs.FindStringSubmatch(it); len(d) > 0 {
			s := d[1]
			b := d[2]
			if _, ok := g_filter[s]; ok {
				continue
			}
			bl[b] = s
		}
	}
	return bl
}

func loadSectionList(c *Config) {
	slch := getServerCh()
	sl := <-slch

	for _, sec := range c.section {
		sec.slch = slch
		updateSection(sec, &sl)
	}
}

func getServerCh() <-chan map[string]string {
	ch := make(chan map[string]string, 4)
	go func() {
		tch := time.Tick(600 * time.Second)
		sl := getServer()
		for {
			select {
			case <-tch:
				sl = getServer()
			default:
				ch <- sl
			}
		}
	}()
	return ch
}

func updateSection(sec *Section, sl *map[string]string) {
	var nich Nich
	sec.sl = make(map[string][]Nich)
	for _, board := range sec.bl {
		if server, ok := (*sl)[board]; ok {
			nich.server = server
			nich.board = board
			if it, ok2 := sec.sl[server]; ok2 {
				sec.sl[server] = append(it, nich)
			} else {
				sec.sl[server] = append(make([]Nich, 0, 1), nich)
			}
		}
	}
}

func threadResList(nich Nich, cache get2ch.Cache) map[string]int {
	data, err := cache.GetData(nich.server, nich.board, "")
	h := make(map[string]int)
	if err != nil { return h }
	list := strings.Split(string(data), "\n")
	for _, it := range list {
		if da := g_reg_dat.FindStringSubmatch(it); len(da) > 0 {
			if d := g_reg_res.FindStringSubmatch(it); len(d) > 0 {
				m, _ := strconv.Atoi(d[1])
				h[da[1]] = m
			}
		}
	}
	return h
}

func readConfig() *Config {
	c := &Config{v: make(map[string]interface{})}
	argc := len(os.Args)
	var path string
	if argc == 2 {
		path = os.Args[1]
	} else {
		path = CONFIG_JSON_PATH_DEF
	}
	err := c.readConfig(path)
	if err != nil {
		c.readDefault()
	}
	return c
}

func (c *Config) readConfig(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil { return err }
	err = json.Unmarshal(data, &c.v)
	if err != nil { return err }

	str := c.getDataString("DBHost", "")
	if str != "" {
		c.db = &DataBase{host: str}
		c.db.port = c.getDataInt("DBPort", 3306)
		c.db.user = c.getDataString("DBUser", "root")
		c.db.pass = c.getDataString("DBPass", "passwd")
		c.db.name = c.getDataString("DBName", "test")
	}

	l := c.getDataArray("SectionList", nil)
	if l != nil {
		c.section = make([]*Section, 0, 1)
		for _, it := range l {
			if data, ok := it.(map[string]interface{}); ok {
				subc := &Config{v: data}
				sec := &Section{
					sc	: SalamiConfig{
						host	: subc.getDataString("SalamiHost", ""),
						port	: subc.getDataInt("SalamiPort", 80),
					},
					bl	: subc.getDataStringArray("BoardList", nil),
				}
				c.section = append(c.section, sec)
			}
		}
	}

	return nil
}

func (c *Config) readDefault() {
}

func (c *Config) getDataInt(h string, def int) (ret int) {
	ret = def
	if it, ok := c.v[h]; ok {
		if f, err := it.(float64); err {
			ret = int(f)
		}
	}
	return
}

func (c *Config) getDataString(h, def string) (ret string) {
	ret = def
	if it, ok := c.v[h]; ok {
		if ret, ok = it.(string); !ok {
			ret = def
		}
	}
	return
}

func (c *Config) getDataArray(h string, def []interface{}) (ret []interface{}) {
	ret = def
	if it, ok := c.v[h]; ok {
		if arr, ok := it.([]interface{}); ok {
			ret = arr
		}
	}
	return
}

func (c *Config) getDataStringArray(h string, def []string) (ret []string) {
	sil := c.getDataArray(h, nil)

	ret = def
	if sil != nil {
		ret = make([]string, 0, 1)
		for _, it := range sil {
			if s, ok := it.(string); ok {
				ret = append(ret, s)
			} else {
				ret = def
				break
			}
		}
	}
	return
}

