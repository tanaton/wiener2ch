package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
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
	dbname		string
}

type Session struct {
	db			mysql.Conn
	get			*get2ch.Get2ch
}

const GO_THREAD_SLEEP_TIME = 2 * time.Second
const THREAD_SLEEP_TIME = 4 * time.Second

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
	var sl, nsl map[string][]Nich

	dbconf := &DataBase{}
	sync := make(chan string)
	// メイン処理
	sl = getServer()
	for key := range sl {
		if h, ok := sl[key]; ok {
			go mainThread(key, h, dbconf, sync)
			time.Sleep(GO_THREAD_SLEEP_TIME)
		}
	}
	for {
		// 処理を止める
		key := <-sync
		nsl = getServer()
		for k := range nsl {
			if _, ok := sl[k]; !ok {
				go mainThread(k, nsl[k], dbconf, sync)
				time.Sleep(GO_THREAD_SLEEP_TIME)
			}
		}
		if h, ok := nsl[key]; ok {
			go mainThread(key, h, dbconf, sync)
			time.Sleep(GO_THREAD_SLEEP_TIME)
		}
		sl = nsl
	}
}

func mainThread(key string, bl []Nich, c *DataBase, sync chan string) {
	ses := &Session{
		db		: mysql.New("tcp", "", fmt.Sprintf("%s:%d", c.host, c.port), c.user, c.pass, c.dbname),
		get		: get2ch.NewGet2ch(get2ch.NewFileCache("/2ch/dat")),
	}
	//ses.get.SetSalami("", 80)
	err := ses.db.Connect()
	if err != nil { ses.db = nil }

	for _, nich := range bl {
		// 板の取得
		tl := ses.getBoard(nich)
		bid := ses.getMysqlBoardNo(nich.board)
		if tl != nil && len(tl) > 0 {
			// スレッドの取得
			ses.getThread(tl, bid)
		}
		// 止める
		time.Sleep(THREAD_SLEEP_TIME)
	}
	sync <- key
}

func (ses *Session) getBoard(nich Nich) []Nich {
	err := ses.get.SetRequest(nich.server, nich.board, "")
	if err != nil { return nil }
	h := threadResList(nich, ses.get.Cache)
	data, err := ses.get.GetData()
	if err != nil {
		log.Printf(err.Error() + "\n")
		return nil
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
	return vect
}

func (ses *Session) getThread(tl []Nich, bid int) {
	for _, nich := range tl {
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

func getServer() map[string][]Nich {
	var nich Nich
	get := get2ch.NewGet2ch(get2ch.NewFileCache("/2ch/dat"))
	sl := make(map[string][]Nich)
	data, err := get.GetServer()
	if err != nil {
		data, err = get.Cache.GetData("", "", "")
		if err != nil { panic(err.Error()) }
	}
	list := strings.Split(string(data), "\n")
	for _, it := range list {
		if d := g_reg_bbs.FindStringSubmatch(it); len(d) > 0 {
			nich.server = d[1]
			nich.board = d[2]
			if _, ok := g_filter[nich.server]; ok {
				continue
			}
			if h, ok := sl[nich.server]; ok {
				sl[nich.server] = append(h, nich)
			} else {
				sl[nich.server] = append(make([]Nich, 0, 1), nich)
			}
		}
	}
	return sl
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

