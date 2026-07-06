// Command thundergo runs the Telegram File-to-Link service: a multi-client
// bot pool that turns Telegram files into HTTP direct links with Range support.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fyaz05/ThunderGo/internal/app"
	tghttp "github.com/fyaz05/ThunderGo/internal/http"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe localhost:<port>/health and exit")
	flag.Parse()

	if *healthcheck {
		os.Exit(runHealthcheck())
	}

	printBanner(tghttp.Version)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := app.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "startup error: %v\n", err)
		os.Exit(1)
	}
	if err := a.Run(); err != nil {
		slog.Error("run error", "error", err)
		os.Exit(1)
	}
	<-ctx.Done()
}

// runHealthcheck probes localhost:<port>/health for Docker HEALTHCHECK.
func runHealthcheck() int {
	port := "8080"
	switch {
	case os.Getenv("TG_HTTP_PORT") != "":
		port = os.Getenv("TG_HTTP_PORT")
	case os.Getenv("PORT") != "":
		port = os.Getenv("PORT")
	case os.Getenv("TG_URL") != "":
		if p := portFromURL(os.Getenv("TG_URL")); p != "" {
			port = p
		}
	}
	url := fmt.Sprintf("http://localhost:%s/health", port)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 1
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusOK {
		return 0
	}
	return 1
}

// printBanner writes the ASCII-art banner to stderr.
func printBanner(version string) {
	fmt.Fprint(os.Stderr, banner)
	fmt.Fprintf(os.Stderr, "\n  v%s · github.com/fyaz05/ThunderGo\n\n", version)
}

// banner is the ASCII art (concatenated strings to avoid backtick escaping).
const banner = " .--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--.\n" +
	"/ .. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\\n" +
	"\\ \\/`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'/\n" +
	" \\/ /`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'\\/ /\n" +
	" / /\\                                                                                    / /\\\n" +
	"/ /\\ \\  ████████╗██╗  ██╗██╗   ██╗███╗   ██╗██████╗ ███████╗██████╗  ██████╗  ██████╗   / /\\ \\\n" +
	"\\ \\/ /  ╚══██╔══╝██║  ██║██║   ██║████╗  ██║██╔══██╗██╔════╝██╔══██╗██╔════╝ ██╔═══██╗  \\ \\/ /\n" +
	" \\/ /      ██║   ███████║██║   ██║██╔██╗ ██║██║  ██║█████╗  ██████╔╝██║  ███╗██║   ██║   \\/ /\n" +
	" / /\\      ██║   ██╔══██║██║   ██║██║╚██╗██║██║  ██║██╔══╝  ██╔══██╗██║   ██║██║   ██║   / /\\\n" +
	"/ /\\ \\     ██║   ██║  ██║╚██████╔╝██║ ╚████║██████╔╝███████╗██║  ██║╚██████╔╝╚██████╔╝  / /\\ \\\n" +
	"\\ \\/ /     ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═════╝ ╚══════╝╚═╝  ╚═╝ ╚═════╝  ╚═════╝   \\ \\/ /\n" +
	" \\/ /                                                                                    \\/ /\n" +
	" / /\\.--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--..--./ /\\\n" +
	"/ /\\ \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\.. \\/\\ \\\n" +
	"\\ `'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'\\`'/\n" +
	" `--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'`--'\n"

// portFromURL extracts the port from a URL string, or "" if absent.
func portFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Port()
}
