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
	"sync"
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
	dbc			*DataBase
	bl			[]string
	sl			map[string][]Nich
	bbn			bool
	bbnLimit	time.Time
	bbnMux		sync.RWMutex
}

type Config struct {
	v			map[string]interface{}
	section		[]*Section
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
	DAT_DIR					= "/2ch/dat"
	GO_THREAD_SLEEP_TIME	= 2 * time.Second
	THREAD_SLEEP_TIME		= 4 * time.Second
	RESTART_SLEEP_TIME		= 60 * time.Second
	SERVER_CYCLE_TIME		= 60 * time.Minute
	BBN_LIMIT_TIME			= 10 * time.Minute
	CONFIG_JSON_PATH_DEF	= "wiener.json"
)

var g_reg_bbs *regexp.Regexp = regexp.MustCompile("(.+\\.2ch\\.net|.+\\.bbspink\\.com)/(.+)<>")
var g_reg_dat *regexp.Regexp = regexp.MustCompile("^(\\d{9,10})\\.dat<>.* \\(([0-9]+)\\)$")
var g_reg_date *regexp.Regexp = regexp.MustCompile("^.*?<>.*?<>.*?([0-9]{4})\\/([0-9]{2})\\/([0-9]{2}).*?([0-9]{2}):([0-9]{2}):([0-9]{2})")

var g_filter map[string]bool = map[string]bool{
	"ipv6.2ch.net"			: true,
	"headline.2ch.net"		: true,
	"headline.bbspink.com"	: true,
}

func main() {
	slch := getServerCh()
	sync := make(chan *Section)

	// 起動
	start(slch, sync)

	for {
		// 処理を止める
		sec := <-sync
		sec.updateSection(<-slch)
		go sec.mainSection(sync)
	}
}

func start(slch <-chan *map[string]string, sync chan<- *Section) {
	sl := <-slch
	seclist := readConfig(sl)
	// メイン処理
	for _, sec := range seclist {
		sec.updateSection(sl)
		go sec.mainSection(sync)
		time.Sleep(GO_THREAD_SLEEP_TIME)
	}
}

func (sec *Section) mainSection(sync chan<- *Section) {
	pch := make(chan *Packet, 4)
	fch := make(chan bool)
	defer func() {
		if r := recover(); r != nil {
			// panic対策
			log.Printf("Panic!! %v", r)
		}
		if checkOpen(fch) {
			close(fch)
		}
		close(pch)
		sync <- sec
	}()

	for key := range sec.sl {
		go sec.mainThread(key, pch, fch)
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
			go sec.mainThread(pack.key, pch, fch)
		}
		time.Sleep(GO_THREAD_SLEEP_TIME)
	}
	time.Sleep(RESTART_SLEEP_TIME)
}

func (sec *Section) mainThread(key string, pch chan *Packet, fch <-chan bool) {
	jmp := false
	ses := &Session{
		get		: get2ch.NewGet2ch(get2ch.NewFileCache(DAT_DIR)),
	}
	if sec.sc.host != "" {
		ses.get.SetSalami(sec.sc.host, sec.sc.port)
	}

	bl, ok := sec.sl[key]
	if ok == false { return }

	if sec.dbc != nil {
		ses.db = mysql.New("tcp", "", fmt.Sprintf("%s:%d", sec.dbc.host, sec.dbc.port), sec.dbc.user, sec.dbc.pass, sec.dbc.name)
		err := ses.db.Connect()
		if err != nil {
			ses.db = nil
		} else {
			// DB接続に成功した場合、閉じる予約をしておく
			defer ses.db.Close()
		}
	}
	// バーボン判定
	sec.checkBourbon(ses.get)

	for _, nich := range bl {
		if !checkOpen(fch) { return }
		// 板の取得
		tl, err := ses.getBoard(nich)
		if err == nil {
			if tl != nil && len(tl) > 0 {
				bid := ses.getMysqlBoardNo(nich.board)
				// スレッドの取得
				if !ses.getThread(tl, bid, fch) { return }
			}
		} else {
			// 板が移転した可能性あり
			jmp = true
		}
		// バーボン判定
		sec.checkBourbon(ses.get)
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

func (sec *Section) checkBourbon(get *get2ch.Get2ch) {
	sec.bbnMux.Lock()
	defer sec.bbnMux.Unlock()

	if sec.bbn {
		// すでにバーボン
		if time.Now().After(sec.bbnLimit) {
			// バーボン解除
			sec.bbn = false
			get.Bourbon = false
		} else {
			// バーボン継続
			get.Bourbon = true
		}
	} else {
		if get.Bourbon {
			// 新しくバーボン
			sec.bbn = true
			// 10分後までバーボン
			sec.bbnLimit = time.Now().Add(BBN_LIMIT_TIME)
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
		log.Printf(err.Error())
		return nil, err
	}
	ses.get.GetBoardName()
	vect := make([]Nich, 0, 1)
	list := strings.Split(string(data), "\n")
	for _, it := range list {
		if d := g_reg_dat.FindStringSubmatch(it); len(d) == 3 {
			var n Nich
			n.server = nich.server
			n.board = nich.board
			n.thread = d[1]
			if m, ok := h[d[1]]; ok {
				if j, err := strconv.Atoi(d[2]); err == nil && j > m {
					vect = append(vect, n)
				}
			} else {
				vect = append(vect, n)
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
			log.Printf(err.Error())
			log.Printf("%s/%s/%s", nich.server, nich.board, nich.thread)
		} else {
			log.Printf("%d OK %s/%s/%s", ses.get.Info.GetCode(), nich.server, nich.board, nich.thread)
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
	bid = -1

	if ses.db != nil {
		bid = ses.getMysqlBoardNoQuery(bname)
		if bid < 0 {
			err := ses.setMysqlBoardNoQuery(bname)
			if err != nil { return }
			bid = ses.getMysqlBoardNoQuery(bname)
		}
	}
	return
}

func (ses *Session) getMysqlBoardNoQuery(bname string) (bid int) {
	bid = -1
	rows, _, err := ses.db.Query("SELECT id FROM boardlist WHERE board = '%s'", bname)
	if err != nil { return }

	for _, row := range rows {
		bid = row.Int(0)
		break
	}
	return
}

func (ses *Session) setMysqlBoardNoQuery(bname string) error {
	_, _, err := ses.db.Query("INSERT INTO boardlist (board) VALUES('%s')", bname)
	return err
}

func (ses *Session) getMysqlThreadNo(data []byte, bid int, tstr string) (tid int) {
	tid = -1

	if ses.db != nil {
		tid = ses.getMysqlThreadNoQuery(bid, tstr)
		if tid < 0 {
			index := bytes.IndexByte(data, '\n')
			bl := bytes.Split(data[0:index], []byte{'<','>'})
			if len(bl) <= 4 { return }
			title, err := sjisToUtf8(bl[4])
			if err != nil { return }
			err = ses.setMysqlThreadNoQuery(bid, tstr, title)
			if err != nil { return }
			tid = ses.getMysqlThreadNoQuery(bid, tstr)
		}
	}
	return
}

func (ses *Session) getMysqlThreadNoQuery(bid int, tstr string) (tid int) {
	tid = -1
	rows, _, err := ses.db.Query("SELECT id FROM threadlist WHERE thread=%s AND board_id=%d", tstr, bid)
	if err != nil { return }

	for _, row := range rows {
		tid = row.Int(0)
		break
	}
	return
}

func (ses *Session) setMysqlThreadNoQuery(bid int, tstr, title string) error {
	_, _, err := ses.db.Query("INSERT INTO threadlist (board_id, thread, title) VALUES(%d,%s,'%s')", bid, tstr, ses.db.EscapeString(title))
	return err
}


func (ses *Session) setMysqlRes(data, moto []byte, n Nich, bid int) {
	if data == nil { return }
	tid := ses.getMysqlThreadNo(data, bid, n.thread)
	if tid < 0 { return }
	if moto == nil {
		ses.setMysqlResQuery(data, tid, 0)
	} else {
		mlen := len(moto)
		if len(data) > (mlen + 1) {
			resno := bytes.Count(moto, []byte{'\n'})
			ses.setMysqlResQuery(data[mlen:], tid, resno)
		}
	}
}

func (ses *Session) setMysqlResQuery(data []byte, tid, resno int) {
	str, err := sjisToUtf8(data)
	if err != nil { return }

	ql := make([]string, 0, 1)
	query := "INSERT INTO restime (thread_id,number,date) VALUES"
	list := strings.Split(str, "\n")
	for _, it := range list {
		resno++
		q, err := ses.createDateQuery(tid, resno, it)
		if err == nil {
			ql = append(ql, q)
		}
	}
	l := len(ql)
	if l > 0 {
		query += strings.Join(ql, ",")
		_, _, err := ses.db.Query(query)
		if err != nil {
			log.Printf("error: mysql挿入失敗")
		} else {
			log.Printf("mysql挿入数:%d", l)
		}
	}
}

func (ses *Session) createDateQuery(tid, resno int, line string) (str string, err error) {
	if d := g_reg_date.FindStringSubmatch(line); len(d) > 6 {
		str = fmt.Sprintf("(%d,%d,'%s%s%s%s%s%s')", tid, resno, d[1], d[2], d[3], d[4], d[5], d[6])
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
	get := get2ch.NewGet2ch(get2ch.NewFileCache(DAT_DIR))
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

func getServerCh() <-chan *map[string]string {
	ch := make(chan *map[string]string, 4)
	go func() {
		tch := time.Tick(SERVER_CYCLE_TIME)
		sl := getServer()
		for {
			select {
			case <-tch:
				sl = getServer()
			default:
				ch <- &sl
			}
		}
	}()
	return ch
}

func (sec *Section) updateSection(sl *map[string]string) {
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
	h := make(map[string]int)
	data, err := cache.GetData(nich.server, nich.board, "")
	if err != nil { return h }
	list := strings.Split(string(data), "\n")
	for _, it := range list {
		if d := g_reg_dat.FindStringSubmatch(it); len(d) == 3 {
			if m, err := strconv.Atoi(d[2]); err == nil {
				h[d[1]] = m
			}
		}
	}
	return h
}

func readConfig(sl *map[string]string) []*Section {
	c := &Config{v: make(map[string]interface{})}
	argc := len(os.Args)
	var path string
	if argc == 2 {
		path = os.Args[1]
	} else {
		path = CONFIG_JSON_PATH_DEF
	}
	c.read(path, sl)
	return c.section
}

func (c *Config) read(filename string, sl *map[string]string) {
	data, err := ioutil.ReadFile(filename)
	if err != nil { return }
	err = json.Unmarshal(data, &c.v)
	if err != nil { return }

	var dbc *DataBase
	str := c.getDataString("DBHost", "")
	if str != "" {
		dbc = &DataBase{host: str}
		dbc.port = c.getDataInt("DBPort", 3306)
		dbc.user = c.getDataString("DBUser", "root")
		dbc.pass = c.getDataString("DBPass", "passwd")
		dbc.name = c.getDataString("DBName", "test")
	}

	c.section = make([]*Section, 0, 1)
	m := make(map[string]bool)
	l := c.getDataArray("SectionList", nil)
	if l != nil {
		for _, it := range l {
			if data, ok := it.(map[string]interface{}); ok {
				subc := &Config{v: data}
				sec := &Section{
					dbc	: dbc,
					sc	: SalamiConfig{
						host	: subc.getDataString("SalamiHost", ""),
						port	: subc.getDataInt("SalamiPort", 80),
					},
					bl	: subc.getDataStringArray("BoardList", nil),
				}
				if sec.bl != nil {
					for _, board := range sec.bl {
						m[board] = true
					}
				}
				c.section = append(c.section, sec)
			}
		}
	}
	bl := make([]string, 0, 1)
	for b, _ := range *sl {
		if _, ok := m[b]; ok == false {
			// 設定ファイルに記載の無い板だった場合
			bl = append(bl, b)
		}
	}
	if len(bl) > 0 {
		sec := &Section{
			dbc	: dbc,
			sc	: SalamiConfig{
				host	: "",
				port	: 80,
			},
			bl	: bl,
		}
		c.section = append(c.section, sec)
	}
	if len(c.section) == 0 {
		c.section = nil
	}
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

