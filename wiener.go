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
	"sort"
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

type DataBase struct {
	host string
	port int
	user string
	pass string
	name string
}

type SalamiConfig struct {
	host string
	port int
}

type Section struct {
	sc  SalamiConfig
	dbc *DataBase
	bl  []string
	sl  map[string]Nichs
	bbn bool
}

type Config struct {
	v       map[string]interface{}
	section []*Section
}

type Session struct {
	db       mysql.Conn
	get      *get2ch.Get2ch
	bbn      bool
	bbnLimit time.Time
}

type Packet struct {
	key string
	jmp bool
}

const (
	DAT_DIR              = "/2ch/dat"
	GO_THREAD_SLEEP_TIME = 2 * time.Second
	THREAD_SLEEP_TIME    = 4 * time.Second
	RESTART_SLEEP_TIME   = 3 * time.Minute
	SERVER_CYCLE_TIME    = 60 * time.Minute
	BBN_LIMIT_TIME       = 10 * time.Minute
	CONFIG_JSON_PATH_DEF = "wiener.json"
)

var stdlog *log.Logger = log.New(os.Stdout, "", log.LstdFlags)

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

	for {
		// 処理を止める
		sec := <-sync
		log.Printf("main() 鯖移転 %v", sec)
		<-slch
		sl := <-slch
		sec.updateSection(sl)
		go sec.mainSection(sync)
	}
}

func start(slch <-chan map[string]string, sync chan<- *Section) {
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
	pch := make(chan Packet, len(sec.sl))
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
		bbn: sec.bbn,
	}
	if sec.sc.host != "" {
		ses.get.SetSalami(sec.sc.host, sec.sc.port)
	}

	bl, ok := sec.sl[key]
	if ok == false {
		return
	}

	if sec.dbc != nil {
		ses.db = mysql.New("tcp", "", fmt.Sprintf("%s:%d", sec.dbc.host, sec.dbc.port), sec.dbc.user, sec.dbc.pass, sec.dbc.name)
		err := ses.db.Connect()
		if err != nil {
			log.Printf("mainThread() DB接続失敗 key:%s", key)
			ses.db = nil
		} else {
			// DB接続に成功した場合、閉じる予約をしておく
			defer ses.db.Close()
		}
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
	vect := Nichs{}
	list := strings.Split(string(data), "\n")
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
							vect = append(vect, n)
						}
					} else {
						// 取得していないスレッド
						vect = append(vect, n)
					}
				}
			}
		}
	}
	if len(vect) > 1 {
		sort.Sort(NichsByThreadSince{vect})
	}
	return vect, nil
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
		if err != nil {
			moto = nil
		}
		if moto != nil {
			if bytes.Count(moto, []byte{'\n'}) >= nich.ResNew {
				// すでに取得済みの場合
				continue
			}
		}
		// 読みに行く
		data, err := ses.get.GetData()
		// バーボン判定
		ses.checkBourbon()
		if err != nil {
			stdlog.Printf("Path:%s/%s/%s\tMessage:%s", nich.Server, nich.Board, nich.Thread, err.Error())
		} else {
			code := ses.get.Info.GetCode()
			ret := ""
			if ses.db != nil && data != nil {
				if moto == nil {
					// 初回取得時
					ret = ses.setMysqlTitleQuery(data, nich)
				} else if code/100 == 2 {
					// HTTP Code 200くらい
					ret = ses.setMysqlResQuery(data, nich)
				}
			}
			stdlog.Printf("Code:%d\tPath:%s/%s/%s\tMessage:%s", code, nich.Server, nich.Board, nich.Thread, ret)
		}
		// 4秒止める
		time.Sleep(THREAD_SLEEP_TIME)
	}
	return true
}

func (ses *Session) setMysqlTitleQuery(data []byte, nich *Nich) (ret string) {
	ret = "error"
	str, err := sjisToUtf8(data)
	if err != nil {
		ret = err.Error()
		return
	}

	list := strings.Split(str, "\n")
	linecount := strings.Count(str, "\n")
	if len(list) > 0 {
		query, err := ses.createTitleQuery(list[0], linecount, nich)
		if err == nil {
			_, _, err := ses.db.Query(query)
			if err == nil {
				ret = "insert"
			} else {
				ret = err.Error()
			}
		} else {
			ret = err.Error()
		}
	}
	return
}

func (ses *Session) createTitleQuery(line string, linecount int, nich *Nich) (str string, err error) {
	if title := g_reg_title.FindStringSubmatch(line); len(title) > 2 {
		master := g_reg_tag.ReplaceAllString(title[1], "") // tag
		master = g_reg_url.ReplaceAllString(master, "")    // url
		str = fmt.Sprintf(
			"INSERT INTO thread_title (board,number,title,master,resnum) VALUES('%s',%s,'%s','%s',%d)",
			nich.Board,
			nich.Thread,
			ses.db.EscapeString(title[2]),
			ses.db.EscapeString(utf8Substr(master, 100)),
			linecount)
	} else {
		err = errors.New("reg error")
	}
	return
}

func (ses *Session) setMysqlResQuery(data []byte, nich *Nich) (ret string) {
	ret = "error"
	linecount := bytes.Count(data, []byte{'\n'})
	if linecount > 1 {
		query := fmt.Sprintf(
			"UPDATE thread_title SET resnum=%d WHERE board='%s' AND number=%s",
			linecount,
			nich.Board,
			nich.Thread)
		_, _, err := ses.db.Query(query)
		if err == nil {
			ret = "update"
		} else {
			ret = err.Error()
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

func sjisToUtf8(data []byte) (string, error) {
	r, err := charset.NewReader("cp932", bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	result, err := ioutil.ReadAll(r)
	return string(result), err
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
	list := strings.Split(string(data), "\n")
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
	ch := make(chan map[string]string, 4)
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
	sec.sl = make(map[string]Nichs)
	for _, board := range sec.bl {
		if server, ok := sl[board]; ok {
			n := &Nich{
				Server: server,
				Board:  board,
			}
			if it, ok2 := sec.sl[server]; ok2 {
				sec.sl[server] = append(it, n)
			} else {
				sec.sl[server] = Nichs{
					n,
				}
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

func readConfig(sl map[string]string) []*Section {
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

func (c *Config) read(filename string, sl map[string]string) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &c.v)
	if err != nil {
		return
	}

	var dbc *DataBase
	str := c.getDataString("DBHost", "")
	if str != "" {
		dbc = &DataBase{host: str}
		dbc.port = c.getDataInt("DBPort", 3306)
		dbc.user = c.getDataString("DBUser", "root")
		dbc.pass = c.getDataString("DBPass", "passwd")
		dbc.name = c.getDataString("DBName", "test")
	}

	c.section = []*Section{}
	m := make(map[string]bool)
	l := c.getDataArray("SectionList", nil)
	if l != nil {
		for _, it := range l {
			if data, ok := it.(map[string]interface{}); ok {
				subc := &Config{v: data}
				sec := &Section{
					dbc: dbc,
					sc: SalamiConfig{
						host: subc.getDataString("SalamiHost", ""),
						port: subc.getDataInt("SalamiPort", 80),
					},
					bl: subc.getDataStringArray("BoardList", nil),
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
	bl := []string{}
	for b, _ := range sl {
		if _, ok := m[b]; ok == false {
			// 設定ファイルに記載の無い板だった場合
			bl = append(bl, b)
		}
	}
	if len(bl) > 0 {
		sec := &Section{
			dbc: dbc,
			sc: SalamiConfig{
				host: "",
				port: 80,
			},
			bl: bl,
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
		ret = []string{}
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
