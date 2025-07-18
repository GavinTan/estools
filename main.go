package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

const version = "v1.0.2"

var (
	srcIndex, destIndex, srcAddr, destAddr                 string
	copySettings, copyMappings, copyAll, help, showVerison bool
	client                                                 = &http.Client{
		Timeout: 10 * time.Second,
	}
)

func init() {
	rootCmd.PersistentFlags().BoolVarP(&help, "help", "h", false, "帮助信息")
	rootCmd.PersistentFlags().BoolVarP(&showVerison, "verison", "v", false, "版本信息")
	rootCmd.PersistentFlags().StringVarP(&srcIndex, "src_index", "x", "", "源索引名称")
	rootCmd.PersistentFlags().StringVarP(&destIndex, "dest_index", "y", "", "目标索引名称")
	rootCmd.PersistentFlags().StringVarP(&srcAddr, "src_addr", "s", "http://localhost:9200", "源ES服务器地址")
	rootCmd.PersistentFlags().StringVarP(&destAddr, "dest_addr", "d", "http://localhost:9201", "目标ES服务器地址")
	rootCmd.PersistentFlags().BoolVarP(&copyAll, "all", "a", false, "拷贝源索引的mappings与settings")
	rootCmd.PersistentFlags().BoolVarP(&copyMappings, "copy_mappings", "", false, "拷贝源索引的mappings")
	rootCmd.PersistentFlags().BoolVarP(&copySettings, "copy_settings", "", false, "拷贝源索引的settings")
}

var rootCmd = &cobra.Command{
	Use:   "estools",
	Short: "ES工具",
	Run: func(cmd *cobra.Command, args []string) {
		if showVerison {
			fmt.Println(version)
			return
		}
		if srcIndex == "" {
			fmt.Println("error: --src_index is required, type --help for more details")
			return
		}
		if copyAll {
			settings := getSettings()
			mappings := getMappings()

			for k, v := range mappings {
				settings[k] = v
			}
			syncData(settings)
			return
		}
		if copyMappings {
			mappings := getMappings()
			syncData(mappings)
			return
		}
		if copySettings {
			settings := getSettings()
			syncData(settings)
			return
		}

		cmd.Help()
	},
}

func getSettings() map[string]interface{} {
	resp, err := client.Get(fmt.Sprintf("%s/%s/_settings", srcAddr, srcIndex))
	if err != nil {
		fmt.Println("get settings: ", err)
		os.Exit(1)
	}

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("get settings: ", string(body))
		os.Exit(1)
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var data map[string]interface{}
	json.Unmarshal(body, &data)

	var settings map[string]interface{}
	if indexSetting, ok := data[srcIndex]; ok {
		settings = indexSetting.(map[string]interface{})
	} else {
		for _, indexSetting := range data {
			settings = indexSetting.(map[string]interface{})
			break
		}
	}

	// 清理es内部生成的设置元数据
	if indexSettings, ok := settings["settings"].(map[string]interface{}); ok {
		if indexMeta, ok := indexSettings["index"].(map[string]interface{}); ok {
			delete(indexMeta, "creation_date")
			delete(indexMeta, "uuid")
			delete(indexMeta, "version")
			delete(indexMeta, "provided_name")
			delete(indexMeta, "resize")

			if routing, ok := indexMeta["routing"].(map[string]interface{}); ok {
				if allocation, ok := routing["allocation"].(map[string]interface{}); ok {
					delete(allocation, "initial_recovery")
				}
			}
		}
	}

	return settings
}

func getMappings() map[string]interface{} {
	resp, err := client.Get(fmt.Sprintf("%s/%s/_mappings", srcAddr, srcIndex))
	if err != nil {
		fmt.Println("get mappings: ", err)
		os.Exit(1)
	}

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("get mappings: ", string(body))
		os.Exit(1)
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	json.Unmarshal(body, &data)

	var mappings map[string]interface{}
	if indexMapping, ok := data[srcIndex]; ok {
		mappings = indexMapping.(map[string]interface{})
	} else {
		for _, indexMapping := range data {
			mappings = indexMapping.(map[string]interface{})
			break
		}
	}

	return mappings
}

func syncData(data map[string]interface{}) {
	if destIndex == "" {
		destIndex = srcIndex
	}

	url := fmt.Sprintf("%s/%s", destAddr, destIndex)
	sv, _ := strconv.Atoi(string(getEsVersion(srcAddr)[0]))
	dv, _ := strconv.Atoi(string(getEsVersion(destAddr)[0]))

	if sv < 7 && dv >= 7 {
		url = fmt.Sprintf("%s/%s?include_type_name=true", destAddr, destIndex)
	}

	if dv < 7 {
		if _, ok := data["mappings"].(map[string]interface{})["properties"]; ok {
			data["mappings"] = map[string]interface{}{
				"_doc": data["mappings"],
			}
		}
	}

	body := bytes.Buffer{}
	json.NewEncoder(&body).Encode(data)

	req, _ := http.NewRequest("PUT", url, &body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("put data: ", err)
		os.Exit(1)
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)

		limit := 300
		err := string(body)
		if len(err) > limit {
			err = err[:limit] + "..."
		}
		fmt.Println("put data: ", err)
		os.Exit(1)
	}
}

func getEsVersion(addr string) string {
	resp, err := client.Get(addr)
	if err != nil {
		fmt.Println("get esVersion: ", err)
		os.Exit(1)
	}

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("get esVersion: ", string(body))
		os.Exit(1)
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	json.Unmarshal(body, &data)

	version := data["version"].(map[string]interface{})["number"].(string)
	return version
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
