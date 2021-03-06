package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Chronostasys/raft"
	"github.com/Chronostasys/raft/kvraft"
	"github.com/nsf/termbox-go"
)

var (
	x    = 0
	y    = 0
	evCh = make(chan termbox.Event)
)

func readEnv() {
	eps := os.Getenv("EPS")
	args := []string{}
	if len(eps) > 0 {
		epl := strings.Split(eps, ";")
		args = append(args, epl...)
	}
	if len(os.Args) == 1 {
		os.Args = append(os.Args, args...)
	}
}

func dojob(job func()) bool {
	errCh := make(chan struct{})
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case ev := <-evCh:
				if ev.Type == termbox.EventKey && (ev.Key == termbox.KeyCtrlC || ev.Key == termbox.KeyCtrlZ) {
					close(errCh)
					return
				}
			case <-doneCh:
				close(errCh)
				return
			}
		}
	}()
	go func() {
		job()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		return true
	case <-errCh:
		return false

	}
}

func scrollDown() {
	w, h := termbox.Size()

	for x := 0; x < w; x++ {
		for y := 0; y < h; y++ {
			if y == h-1 {
				termbox.SetCell(x, y, ' ', termbox.ColorDefault, termbox.ColorDefault)
			} else {
				cell := termbox.GetCell(x, y+1)
				termbox.SetCell(x, y, cell.Ch, cell.Fg, cell.Bg)
			}
		}
	}
	y--
}
func checkHeight() {
	termbox.Flush()
	_, wy := termbox.Size()
	if y >= wy {
		scrollDown()
		termbox.Flush()
	}
}

func main() {
	readEnv()
	ends := os.Args[1:]
	if len(ends) == 0 {
		println("kvraft cli")
		println("Usage:")
		println("	$cli [endpoint1] [endpoint2] [endpoint3]...")
		println("Example:")
		println("	$cli :1234 :1235 :1236")
		os.Exit(1)
	}
	rpcends := raft.MakeRPCEnds(ends)
	client := kvraft.MakeClerk(rpcends)
	hisID := 0
	history := []string{}
	commands := []string{
		"help",
		"exit",
		"append",
		"get",
		"put",
		"larger",
	}
	setCurWrap := func() {
		mx, _ := termbox.Size()
		termbox.SetCursor(x%mx, y+x/mx)
		termbox.Flush()
	}
	err := termbox.Init()
	if err != nil {
		panic(err)
	}
	defer termbox.Close()

	go func() {
		for {
			ev := termbox.PollEvent()
			evCh <- ev
		}
	}()
	for {
		checkHeight()
		x = 0
		lead := "cli> "
		for _, v := range lead {
			termbox.SetCell(x, y, v, termbox.ColorLightGray, termbox.ColorDefault)
			x++
		}
		termbox.Flush()
		// Read the keyboad input.
		line := ""
	READLINE:
		for {
			setCurWrap()
			ev := <-evCh
			switch ev.Type {

			case termbox.EventKey:
				switch ev.Key {
				case termbox.KeyTab:
					for _, v := range commands {
						if len(v) > len(line) && v[:len(line)] == line {
							line = v
							x = 5
							for i, v := range line {
								if strings.Contains(line[:i], " ") {
									setCellWrap(5+i, y, v, termbox.ColorGreen, termbox.ColorDefault)
								} else {
									setCellWrap(5+i, y, v, termbox.ColorLightBlue, termbox.ColorDefault)
								}
								x++
							}
							break
						}
					}
					line += " "
					setCellWrap(x, y, ' ', termbox.ColorDefault, termbox.ColorDefault)
					x++
					continue

				case termbox.KeyEsc:
					return
				case termbox.KeyEnter:
					history = append(history, line)
					hisID = len(history)
					mx, _ := termbox.Size()
					y += x / mx
					y++
					println()

					n := processInput(line, client)
					if n == -1 {
						return
					}
					break READLINE
				case termbox.KeyBackspace, termbox.KeyBackspace2:
					if x <= 5 {
						continue
					}
					x--
					for i, v := range line[x-4:] {
						setCellWrap(x+i, y, v, termbox.ColorDefault, termbox.ColorDefault)
					}
					setCellWrap(4+len(line), y, ' ', termbox.ColorDefault, termbox.ColorDefault)
					line = line[:x-5] + line[x-4:]
				case termbox.KeyArrowLeft:
					if x-5 > 0 {
						x--
					}
				case termbox.KeyArrowRight:
					if x-5 < len(line) {
						x++
					}
				case termbox.KeyArrowUp:
					if hisID == 0 {
						continue
					}
					if hisID == len(history) && len(strings.Trim(line, " ")) != 0 {
						history = append(history, line)
					}
					hisID--
					for i := range line {
						setCellWrap(5+i, y, ' ', termbox.ColorDefault, termbox.ColorDefault)
					}
					line = history[hisID]
					for i, v := range line {
						if strings.Contains(line[:i], " ") {
							setCellWrap(5+i, y, v, termbox.ColorGreen, termbox.ColorDefault)
						} else {
							setCellWrap(5+i, y, v, termbox.ColorLightBlue, termbox.ColorDefault)
						}
					}
					x = 5
				case termbox.KeyArrowDown:
					if hisID >= len(history)-1 {
						continue
					}
					hisID++
					for i := range line {
						setCellWrap(5+i, y, ' ', termbox.ColorDefault, termbox.ColorDefault)
					}
					line = history[hisID]
					for i, v := range line {
						if strings.Contains(line[:i], " ") {
							setCellWrap(5+i, y, v, termbox.ColorGreen, termbox.ColorDefault)
						} else {
							setCellWrap(5+i, y, v, termbox.ColorLightBlue, termbox.ColorDefault)
						}
					}
					x = 5
				case termbox.KeyCtrlC, termbox.KeyCtrlZ:
					return
				default:
					if ev.Key == termbox.KeySpace {
						ev.Ch = ' '
					}
					color := termbox.ColorLightBlue
					if strings.Contains(line[:x-5], " ") {
						color = termbox.ColorGreen
					}
					setCellWrap(x, y, ev.Ch, color, termbox.ColorDefault)
					x++
					line = line[:x-6] + string(ev.Ch) + line[x-6:]
					for i, v := range line[x-5:] {
						if strings.Contains(line[:x-5+i], " ") {
							setCellWrap(x+i, y, v, termbox.ColorGreen, termbox.ColorDefault)
						} else {
							setCellWrap(x+i, y, v, termbox.ColorLightBlue, termbox.ColorDefault)
						}
					}
					setCurWrap()
				}
			case termbox.EventError:
				return
			default:
				continue
			}
		}
	}
}

func setCellWrap(x, y int, ch rune, fg, bg termbox.Attribute) {
	mx, _ := termbox.Size()
	termbox.SetCell(x%mx, y+x/mx, ch, fg, bg)
}

func tprintln(s string, color termbox.Attribute) {
	tprint(s, color)
	x = 0
	y++
}
func tprint(s string, color termbox.Attribute) {
	checkHeight()
	mx, _ := termbox.Size()
	for _, v := range s {
		termbox.SetCell(x, y, v, color, termbox.ColorDefault)
		x++
		if x == mx {
			x = 0
			y++
			checkHeight()
		}
	}
}

func processInput(input string, client *kvraft.Clerk) int {
	x = 0
	input = strings.Trim(input, "\n\r ")
	if len(strings.Trim(input, " ")) == 0 {
		return 0
	}
	if input == "help" {
		printHelp()
		return 10
	}
	cc := strings.Split(input, "\"")
	cmds := []string{}
	for i, v := range cc {
		if i%2 == 0 {
			v = strings.Trim(v, " ")
			if len(v) == 0 {
				continue
			}
			cmds = append(cmds, strings.Split(v, " ")...)
		} else {
			cmds = append(cmds, v)
		}
	}
	if cmds[0] == "exit" {
		return -1
	}
	if cmds[0] == "append" {
		if len(cmds) != 3 {
			tprintln("invalid input "+input, termbox.ColorRed)
			printHelp()
			return 11
		}
		t := elapsed()
		if !dojob(func() {
			client.Append(strings.Trim(cmds[1], "\""), strings.Trim(cmds[2], "\""))
		}) {
			return -1
		}
		tprint("    Append OK -- ", termbox.ColorDefault)
		tprintln(t(), termbox.ColorLightGreen)
		return 0
	} else if cmds[0] == "put" {
		if len(cmds) != 3 {
			tprintln("invalid input "+input, termbox.ColorRed)
			printHelp()
			return 11
		}
		t := elapsed()
		if !dojob(func() {
			client.Put(strings.Trim(cmds[1], "\""), strings.Trim(cmds[2], "\""))
		}) {
			return -1
		}
		tprint("    Put OK -- ", termbox.ColorDefault)
		tprintln(t(), termbox.ColorLightGreen)
		return 0
	} else if cmds[0] == "get" {
		if len(cmds) != 2 {
			tprintln("invalid input "+input, termbox.ColorRed)
			printHelp()
			return 11
		}
		t := elapsed()
		v := ""
		if !dojob(func() {
			v = client.Get(strings.Trim(cmds[1], "\""))
		}) {
			return -1
		}
		tprint("    Key ", termbox.ColorDefault)
		tprint("\""+cmds[1]+"\"", termbox.ColorGreen)
		tprint(" Val ", termbox.ColorDefault)
		tprintln("\""+v+"\"", termbox.ColorLightBlue)
		tprint("    Get OK -- ", termbox.ColorDefault)
		tprintln(t(), termbox.ColorLightGreen)
		return 1
	} else if cmds[0] == "larger" {
		l := 0
		if !dojob(func() {
			client.Larger(strings.Trim(cmds[1], "\""), 1000, 1000, 0, func(k, v string) bool {
				tprintln(fmt.Sprintf(" Key: %s val: %s", k, v), termbox.ColorDefault)
				l++
				return true
			})
		}) {
			return -1
		}
		return l
	} else {
		tprintln("Unknown command: "+cmds[0], termbox.ColorRed)
		printHelp()
		return 11
	}
}

func printHelp() {
	tprintln("", termbox.ColorYellow)
	tprintln("---------------help-----------------", termbox.ColorYellow)
	tprintln("append [key] [value] - append value", termbox.ColorYellow)
	tprintln("put [key] [value]    - update value", termbox.ColorYellow)
	tprintln("get [key]            - get value", termbox.ColorYellow)
	tprintln("exit                 - exit", termbox.ColorYellow)
	tprintln("Examples:", termbox.ColorYellow)
	tprintln("  put a b", termbox.ColorYellow)
	tprintln("  append a \"b c\"", termbox.ColorYellow)
	tprintln("", termbox.ColorYellow)
}

func elapsed() func() string {
	start := time.Now()
	return func() string {
		return fmt.Sprintf("%v", time.Since(start))
	}
}
