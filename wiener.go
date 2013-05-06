package main

import (
	"bytes"
	"code.google.com/p/go-charset/charset"
	_ "code.google.com/p/go-charset/data"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tanaton/get2ch-go"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/thrsafe"
	"io/ioutil"
	"log"
	"math"
	"os"
	"regexp"
	//"sort"
	"strconv"
	"strings"
	"time"
)

type Nich struct {
	Server       string
	Board        string
	Thread       string
	ThreadNumber int
	ResNew       int
}
type Nichs []*Nich
type NichsByThreadSince struct {
	Nichs
}

type Item struct {
	Board  string
	Number string
	Title  string
	Master string
	Resnum int
	Insert bool
}

type DataBase struct {
	Host string `json:"DBHost"`
	Port int    `json:"DBPort"`
	User string `json:"DBUser"`
	Pass string `json:"DBPass"`
	Name string `json:"DBName"`
}

type SalamiConfig struct {
	Host string `json:"SalamiHost"`
	Port int    `json:"SalamiPort"`
}

type Section struct {
	SalamiConfig
	Bl  []string         `json:"BoardList"`
	Sl  map[string]Nichs `json:"-"`
	Dbc chan<- Item      `json:"-"`
}

type Config struct {
	DataBase
	Sec []*Section `json:"SectionList"`
}

type Session struct {
	dbc      chan<- Item
	get      *get2ch.Get2ch
	bbn      bool
	bbnLimit time.Time
}

type Packet struct {
	key string
	jmp bool
}

const (
	DAT_DIR                   = "/2ch/dat"
	GO_THREAD_SLEEP_TIME      = 2 * time.Second
	THREAD_SLEEP_TIME         = 4 * time.Second
	RESTART_SLEEP_TIME        = 3 * time.Minute
	SERVER_CYCLE_TIME         = 180 * time.Minute
	BBN_LIMIT_TIME            = 10 * time.Minute
	CONFIG_JSON_PATH_DEF      = "wiener.json"
	DB_BUFFER_SIZE            = 128
	LF_STR                    = "\n"
	LF_BYTE              byte = '\n'
	SERVER_CHAN_MAX           = 4
)

var stdlog *log.Logger = log.New(os.Stdout, "Date:", log.LstdFlags)

var g_reg_bbs *regexp.Regexp = regexp.MustCompile("(.+\\.2ch\\.net|.+\\.bbspink\\.com)/(.+)<>")
var g_reg_dat *regexp.Regexp = regexp.MustCompile("^(\\d{9,10})\\.dat<>.* \\(([0-9]+)\\)$")
var g_reg_title *regexp.Regexp = regexp.MustCompile("^.*?<>.*?<>.*?<>(.*?)<>(.*)")
var g_reg_tag *regexp.Regexp = regexp.MustCompile("<\\/?[^>]*>")
var g_reg_url *regexp.Regexp = regexp.MustCompile("(?:s?h?ttps?|sssp):\\/\\/[-_.!~*'()\\w;\\/?:\\@&=+\\$,%#\\|]+")

var g_filter_server map[string]bool = map[string]bool{
	"ipv6.2ch.net":         true,
	"headline.2ch.net":     true,
	"headline.bbspink.com": true,
}

var g_filter_board map[string]bool = map[string]bool{
	"tv2chwiki": true,
}

func (n Nichs) Len() int      { return len(n) }
func (n Nichs) Swap(i, j int) { n[i], n[j] = n[j], n[i] }
func (ts NichsByThreadSince) Less(i, j int) bool {
	// 降順
	return ts.Nichs[i].ThreadNumber > ts.Nichs[j].ThreadNumber
}

func main() {
	slch := getServerCh()
	sync := make(chan *Section)

	// 起動
	start(slch, sync)

	// 処理を止める
	for sec := range sync {
		log.Printf("main() 鯖移転 %v", sec)
		<-slch
		sl := <-slch
		sec.updateSection(sl)
		go sec.mainSection(sync)
	}
}

func start(slch <-chan map[string]string, sync chan<- *Section) {
	sl := <-slch
	conf := readConfig(sl)
	dbc := conf.startDataBase()
	// メイン処理
	for _, sec := range conf.Sec {
		sec.Dbc = dbc
		sec.updateSection(sl)
		go sec.mainSection(sync)
		time.Sleep(GO_THREAD_SLEEP_TIME)
	}
}

func (sec *Section) mainSection(sync chan<- *Section) {
	pch := make(chan Packet, len(sec.Sl))
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

	for key := range sec.Sl {
		go sec.mainThread(key, pch, fch)
		time.Sleep(GO_THREAD_SLEEP_TIME)
	}
	for pack := range pch {
		if pack.jmp {
			// 鯖移転の可能性
			log.Printf("mainSection() 鯖移転 %v", pack)
			close(fch)
			time.Sleep(RESTART_SLEEP_TIME)
			break
		} else {
			if checkOpen(fch) {
				// 移転なし
				log.Printf("mainSection() 移転無し %v", pack)
				go sec.mainThread(pack.key, pch, fch)
			} else {
				log.Printf("mainSection() 終了 %v", pack)
				break
			}
		}
	}
}

func (sec *Section) mainThread(key string, pch chan Packet, fch <-chan bool) {
	jmp := false
	defer func() {
		log.Printf("mainThread() 終了 key:%s", key)
		if checkOpen(fch) {
			// 現在の情報を送信
			pch <- Packet{
				key: key,
				jmp: jmp,
			}
		}
	}()

	ses := &Session{
		get: get2ch.NewGet2ch(get2ch.NewFileCache(DAT_DIR)),
		dbc: sec.Dbc,
	}
	if sec.SalamiConfig.Host != "" {
		ses.get.SetSalami(sec.SalamiConfig.Host, sec.SalamiConfig.Port)
	}

	bl, ok := sec.Sl[key]
	if ok == false {
		return
	}

	for _, nich := range bl {
		if !checkOpen(fch) {
			return
		}
		// 板の取得
		tl, err := ses.getBoard(nich)
		if err == nil {
			// バーボン判定
			ses.checkBourbon()
			if tl != nil && len(tl) > 0 {
				// スレッドの取得
				if !ses.getThread(tl, fch) {
					return
				}
			}
		} else {
			// 板が移転した可能性あり
			log.Printf("mainThread() DB接続失敗 key:%s", key)
			jmp = true
			break
		}
		// 止める
		time.Sleep(THREAD_SLEEP_TIME)
	}
}

func (ses *Session) checkBourbon() {
	if ses.bbn {
		// すでにバーボン
		if time.Now().After(ses.bbnLimit) {
			// バーボン解除
			ses.bbn = false
			ses.get.Bourbon = false
		} else {
			// バーボン継続
			ses.get.Bourbon = true
		}
	} else {
		if ses.get.Bourbon {
			// 新しくバーボン
			ses.bbn = true
			// 10分後までバーボン
			ses.bbnLimit = time.Now().Add(BBN_LIMIT_TIME)
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

func (ses *Session) getBoard(nich *Nich) (Nichs, error) {
	err := ses.get.SetRequest(nich.Server, nich.Board, "")
	if err != nil {
		return nil, err
	}
	// キャッシュのスレッド一覧からレス数を取得
	h := threadResList(nich, ses.get.Cache)
	// 今のスレッド一覧を取得
	data, err := ses.get.GetData()
	if err != nil {
		log.Printf(err.Error())
		return nil, err
	}
	ses.get.GetBoardName()
	list := strings.Split(string(data), LF_STR)
	vect := make(Nichs, len(list) + 1)
	l := 0
	for _, it := range list {
		if d := g_reg_dat.FindStringSubmatch(it); len(d) == 3 {
			n := &Nich{
				Server:       nich.Server,
				Board:        nich.Board,
				Thread:       d[1],
				ThreadNumber: 0,
				ResNew:       0,
			}
			tnum, converr := strconv.ParseInt(n.Thread, 10, 64)
			if (converr == nil) && (tnum >= 0) && (tnum <= math.MaxInt32) {
				// intの範囲を超えるスレッドは扱わない
				n.ThreadNumber = int(tnum)
				if i, err := strconv.Atoi(d[2]); err == nil {
					n.ResNew = i
					if j, ok := h[n.Thread]; ok {
						if i > j {
							// スレッドが更新されている
							vect[l] = n
							l++
						}
					} else {
						// 取得していないスレッド
						vect[l] = n
						l++
					}
				}
			}
		}
	}
	return vect[:l], nil
}

func (ses *Session) getThread(tl Nichs, fch <-chan bool) bool {
	for _, nich := range tl {
		if !checkOpen(fch) {
			return false
		}
		err := ses.get.SetRequest(nich.Server, nich.Board, nich.Thread)
		if err != nil {
			continue
		}
		moto, err := ses.get.Cache.GetData(nich.Server, nich.Board, nich.Thread)
		if err == nil && moto != nil {
			if bytes.Count(moto, []byte{LF_BYTE}) >= nich.ResNew {
				// すでに取得済みの場合
				continue
			}
		} else {
			moto = nil
		}
		// 読みに行く
		data, err := ses.get.GetData()
		// バーボン判定
		ses.checkBourbon()
		if err == nil {
			code := ses.get.Info.GetCode()
			ret := ""
			if data != nil {
				if moto == nil {
					// 初回取得時
					ret = ses.setMysqlTitleQuery(data, nich)
				} else if code/100 == 2 {
					// HTTP Code 200くらい
					ret = ses.setMysqlResQuery(data, nich)
				}
			}
			stdlog.Printf("\tCode:%d\tPath:%s/%s/%s\tMessage:%s", code, nich.Server, nich.Board, nich.Thread, ret)
		} else {
			stdlog.Printf("\tPath:%s/%s/%s\tMessage:%s", nich.Server, nich.Board, nich.Thread, err.Error())
		}
		// 4秒止める
		time.Sleep(THREAD_SLEEP_TIME)
	}
	return true
}

func (ses *Session) setMysqlTitleQuery(data []byte, nich *Nich) (ret string) {
	ret = "insert"
	b, err := sjisToUtf8(data)
	if err != nil {
		ret = err.Error()
		return
	}

	linecount := bytes.Count(b, []byte{LF_BYTE})
	if linecount > 0 {
		index := bytes.IndexByte(b, LF_BYTE)
		err := ses.createTitleQuery(string(b[:index+1]), linecount, nich)
		if err != nil {
			ret = err.Error()
		}
	}
	return
}

func (ses *Session) createTitleQuery(line string, linecount int, nich *Nich) (err error) {
	if title := g_reg_title.FindStringSubmatch(line); len(title) > 2 {
		master := g_reg_tag.ReplaceAllString(title[1], "") // tag
		master = g_reg_url.ReplaceAllString(master, "")    // url
		ses.dbc <- Item{
			Board:  nich.Board,
			Number: nich.Thread,
			Title:  title[2],
			Master: master,
			Resnum: linecount,
			Insert: true,
		}
	} else {
		err = errors.New("reg error")
	}
	return
}

func (ses *Session) setMysqlResQuery(data []byte, nich *Nich) (ret string) {
	ret = "update"
	linecount := bytes.Count(data, []byte{LF_BYTE})
	if linecount > 1 {
		ses.dbc <- Item{
			Board:  nich.Board,
			Number: nich.Thread,
			Resnum: linecount,
			Insert: false,
		}
	}
	return
}

func utf8Substr(s string, max int) string {
	r := []rune(s)
	l := len(r)
	if l > max {
		l = max
	}
	return string(r[:l])
}

func sjisToUtf8(data []byte) (ret []byte, err error) {
	r, e := charset.NewReader("cp932", bytes.NewReader(data))
	if e == nil {
		ret, err = ioutil.ReadAll(r)
	} else {
		err = e
	}
	return
}

func getServer() map[string]string {
	get := get2ch.NewGet2ch(get2ch.NewFileCache(DAT_DIR))
	bl := make(map[string]string)
	data, err := get.GetServer()
	if err != nil {
		data, err = get.Cache.GetData("", "", "")
		if err != nil {
			panic(err.Error())
		}
	}
	list := strings.Split(string(data), LF_STR)
	for _, it := range list {
		if d := g_reg_bbs.FindStringSubmatch(it); len(d) > 0 {
			s := d[1]
			b := d[2]
			if _, ok := g_filter_server[s]; ok {
				continue
			}
			if _, ok := g_filter_board[b]; ok {
				continue
			}
			bl[b] = s
		}
	}
	return bl
}

func getServerCh() <-chan map[string]string {
	ch := make(chan map[string]string, SERVER_CHAN_MAX)
	go func() {
		tch := time.Tick(SERVER_CYCLE_TIME)
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

func (sec *Section) updateSection(sl map[string]string) {
	sec.Sl = make(map[string]Nichs)
	for _, board := range sec.Bl {
		if server, ok := sl[board]; ok {
			n := &Nich{
				Server: server,
				Board:  board,
			}
			if it, ok2 := sec.Sl[server]; ok2 {
				sec.Sl[server] = append(it, n)
			} else {
				sec.Sl[server] = Nichs{n}
			}
		}
	}
}

func threadResList(nich *Nich, cache get2ch.Cache) map[string]int {
	h := make(map[string]int)
	data, err := cache.GetData(nich.Server, nich.Board, "")
	if err != nil {
		return h
	}
	list := strings.Split(string(data), LF_STR)
	for _, it := range list {
		if d := g_reg_dat.FindStringSubmatch(it); len(d) == 3 {
			if m, err := strconv.Atoi(d[2]); err == nil {
				h[d[1]] = m
			}
		}
	}
	return h
}

func readConfig(sl map[string]string) *Config {
	c := &Config{}
	argc := len(os.Args)
	var path string
	if argc == 2 {
		path = os.Args[1]
	} else {
		path = CONFIG_JSON_PATH_DEF
	}
	c.read(path, sl)
	return c
}

func (c *Config) read(filename string, sl map[string]string) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, c)
	if err != nil {
		return
	}

	m := make(map[string]bool)
	for _, it := range c.Sec {
		for _, b := range it.Bl {
			m[b] = true
		}
	}
	bl := []string{}
	for b, _ := range sl {
		if _, ok := m[b]; !ok {
			// 設定ファイルに記載の無い板だった場合
			bl = append(bl, b)
		}
	}
	if len(bl) > 0 {
		c.Sec = append(c.Sec, &Section{
			SalamiConfig: SalamiConfig{
				Host: "",
				Port: 80,
			},
			Bl: bl,
		})
	}
}

func (c *Config) startDataBase() chan<- Item {
	dbc := make(chan Item, DB_BUFFER_SIZE)
	go func() {
		var con mysql.Conn
		for it := range dbc {
			if con == nil {
				con = c.connectDataBase()
			}
			if con != nil && it.Board != "" && it.Number != "" {
				var query string
				if it.Insert {
					query = fmt.Sprintf(
						"INSERT INTO thread_title (board,number,title,master,resnum) VALUES('%s',%s,'%s','%s',%d)",
						it.Board,
						it.Number,
						con.EscapeString(it.Title),
						con.EscapeString(utf8Substr(it.Master, 100)),
						it.Resnum)
				} else {
					query = fmt.Sprintf(
						"UPDATE thread_title SET resnum=%d WHERE board='%s' AND number=%s",
						it.Resnum,
						it.Board,
						it.Number)
				}
				_, _, err := con.Query(query)
				if err != nil {
					log.Printf("mysql query error [%s]", query)
					con.Close()
					con = nil
				}
			}
		}
		if con != nil {
			con.Close()
			con = nil
		}
	}()
	return dbc
}

func (c *Config) connectDataBase() (con mysql.Conn) {
	if c.DataBase.Host != "" {
		con = mysql.New("tcp", "", fmt.Sprintf("%s:%d", c.DataBase.Host, c.DataBase.Port), c.DataBase.User, c.DataBase.Pass, c.DataBase.Name)
		err := con.Connect()
		if err != nil {
			log.Printf("mainThread() DB接続失敗")
			con = nil
		}
	}
	return
}
