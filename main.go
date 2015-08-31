package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/lib/pq"
	"github.com/yosisa/fluxion/buffer"
	"github.com/yosisa/fluxion/message"
	"github.com/yosisa/fluxion/plugin"
)

type Config struct {
	URI     string            `toml:"uri"`
	Table   string            `toml:"table"`
	Mapping map[string]string `toml:"mapping"`
}

type OutPostgres struct {
	env   *plugin.Env
	conf  Config
	db    *sql.DB
	pool  *sync.Pool
	spool []string
}

func (p *OutPostgres) Init(env *plugin.Env) (err error) {
	p.env = env
	if err = env.ReadConfig(&p.conf); err != nil {
		return
	}
	p.pool = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	p.spool = []string{"", "$1"}
	for i, l := 2, len(p.conf.Mapping); i <= l; i++ {
		p.spool = append(p.spool, fmt.Sprintf("%s,$%d", p.spool[i-1], i))
	}
	return
}

func (p *OutPostgres) Start() (err error) {
	p.db, err = sql.Open("postgres", p.conf.URI)
	return
}

func (p *OutPostgres) Encode(ev *message.Event) (buffer.Sizer, error) {
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer p.pool.Put(buf)

	var d data
	for cname, rname := range p.conf.Mapping {
		switch rname {
		case "@tag", "_tag":
			d.add(ev.Tag)
		case "@timestamp", "_timestamp":
			d.add(ev.Time)
		default:
			v, ok := ev.Record[rname]
			if !ok {
				continue
			}
			d.add(v)
		}
		buf.WriteString(cname)
		buf.WriteString(",")
	}
	n := buf.Len()
	if n == 0 {
		return nil, nil
	}
	d.columns = buf.String()[:n-1] // Strip trailing comma
	return &d, nil
}

func (p *OutPostgres) Write(l []buffer.Sizer) (int, error) {
	tx, err := p.db.Begin()
	if err != nil {
		p.env.Log.Error(err)
		return 0, err
	}
	for i, item := range l {
		d := item.(*data)
		n := len(d.values)
		stmt := fmt.Sprintf(`INSERT INTO %s(%s) VALUES(%s)`, p.conf.Table, d.columns, p.spool[n])
		if _, err = tx.Exec(stmt, d.values...); err != nil {
			p.env.Log.Error(err)
			if err = tx.Rollback(); err != nil {
				p.env.Log.Error(err)
				return 0, err
			}
			if i > 0 {
				return p.Write(l[:i])
			}
			return 1, nil // Remove the failed item due to continuous error
		}
	}
	if err = tx.Commit(); err != nil {
		p.env.Log.Error(err)
		return 0, err
	}
	return len(l), nil
}

func (p *OutPostgres) Close() error {
	return p.db.Close()
}

type data struct {
	columns string
	values  []interface{}
}

func (d *data) add(v interface{}) {
	d.values = append(d.values, v)
}

func (d *data) Size() int64 {
	// TODO: compute accurate size
	return int64(len(d.columns) + len(d.values))
}

func main() {
	plugin.New("out-postgres", func() plugin.Plugin {
		return &OutPostgres{}
	}).Run()
}
