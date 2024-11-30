package main

import (
	"bytes"
	"fmt"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"os/exec"
	"strings"
)


const (
	GKFSHOSTFILE  = "tools.gekkofs.hosts_file"
	GKFSDATADIR   = "tools.gekkofs.data_dir"
	GKFSLDPRELOAD = "tools.gekkofs.ld_preload_file"
	GKFSMOUNTDIR  = "tools.gekkofs.mount_dir"
	CLUSHCONFIG   = "tools.clush.conf"
)

// GenerateHostFile
// @Author

// @Description 脚本抓取多节点上的gkfs进程pid，基于gkfs_hosts抓取进程
// @Date
// @Param
// @return
func GenerateHostFile() error {
	log.Println("processing gekkofs hosts file...")
	hostsFile := viper.GetString(GKFSHOSTFILE)
	shellCmd := fmt.Sprintf("cp %s ./gkfs_hosts.txt.bak", hostsFile)

	cmd := exec.Command("bash", "-c", shellCmd)

	if out, err := cmd.CombinedOutput(); err != nil {
		log.Println("execute a command error", err)
		log.Println("err: ", string(out))
	}
	curDir, err := os.Getwd()
	if err != nil {
		return err
	}
	//log.Println(curDir)
	shellCmd = `sort -k 1 -V ./gkfs_hosts.txt.bak -o ./gkfs_hosts.txt`
	var errb bytes.Buffer
	cmd = exec.Command("bash", "-c", shellCmd)
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		log.Println(shellCmd)
		log.Fatal("execute a command error", err)
	}
	clushFile := viper.GetString(CLUSHCONFIG)
	shellCmd = fmt.Sprintf("rm -f %s/gkfs_hosts.txt.pid* 2>/dev/null ;nodes=$(srun hostname|nodeset -f);clush --conf=%s -b -w $nodes 'if [ -f %s ] && [ -f /etc/hostname ] && command -v lsof > /dev/null; then cat %s | grep $(cat /etc/hostname) | while read line; do host=$(echo $line | awk '\\''{print $1}'\\''); url=$(echo $line | awk '\\''{print $2}'\\''); port=$(echo $url | awk -F: '\\''{print $3}'\\''); pid=$(lsof -t -i :$port 2>/dev/null | head -n 1); echo \"$host: $url:${pid:-PID not found}\" >> %s/gkfs_hosts.txt.pid.bak; done; sort -k 1 -V %s/gkfs_hosts.txt.pid.bak -o %s/gkfs_hosts.txt.pid; else echo \"Required gkfs_hosts relation files or some commands are missing\"; fi 1>&2'",
		curDir,
		clushFile,
		hostsFile,
		hostsFile,
		curDir,
		curDir,
		curDir)
	cmd = exec.Command("bash", "-c", shellCmd)
	log.Println(shellCmd)
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		log.Println("execute a command error", err)
		log.Println(shellCmd)
		log.Fatal("err: ", errb.String())
	}

	//log.Println(shellCmd)
	//log.Println("err: ", errb.String())
	return nil
}

/**
 * @Author
 * @Description //调试脚本用
 * @Date
 * @Param
 * @return
 **/
func prepareEnv(cCtx *cli.Context) error {
	shellCmd := `srun hostname xx | nodeset -f`
	cmd := exec.Command("bash", "-c", shellCmd)
	//var outb, errb bytes.Buffer
	//cmd.Stdout = &outb
	//cmd.Stderr = &errb
	//err := cmd.Run()
	b, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatal("execute a command error", err)
	}
	fmt.Println(string(b), len(b))
	//fmt.Println(outb.String())
	//fmt.Println(errb.String())
	//shellCmd = fmt.Sprintf("nodeset -c %s", outb.String())
	//cmd = exec.Command("bash", "-c", shellCmd)
	//cmd.Stdout = &outb
	//log.Println()
	//if err = cmd.Run(); err != nil {
	//	log.Fatal("shell execute error", shellCmd, err)
	//}
	//log.Println(outb.String())
	return nil
}

/**
 * @Author
 * @Description stage-in阶段，从加速层到全局存储导出数据
 * 包含运行环境准备等脚本的处理
 * @Date 10:28AM 7/4/24
 * @Param 包含配置文件和命令行读入的参数。
 * @return error
 **/
func stageIn(ctx *cli.Context) error {

	dataDir := viper.Get(GKFSDATADIR)
	ldPreload := viper.Get(GKFSLDPRELOAD)
	workDir, err := os.Getwd()
	if err != nil {
		return err
	}

	shellCmd := fmt.Sprintf("mpirun -host `srun hostname|nodeset -f|nodeset -e | tr \" \" \",\"` -env LD_PRELOAD %s -env HOST_SIZE `srun hostname|nodeset -f|nodeset -c` -ppn 1 %s/stage-in %s %s %s/gkfs_hosts.txt.pid  %s ",
		ldPreload,
		workDir,
		ctx.Args().Get(0),
		ctx.Args().Get(1),
		workDir,
		dataDir)
	fmt.Println(shellCmd)
	cmd := exec.Command("bash", "-c", shellCmd)
	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err = cmd.Run()
	if err != nil {
		fmt.Println(shellCmd)
		log.Fatal("execute error ", err, " ", errb.String())
	}
	fmt.Println(outb.String())
	return nil
}

/**
 * @Author
 * @Description  stage-in阶段，从加速层到全局存储导出数据，
 * 包含环境准备，抓取环境变量
 * @Date
 * @Param
 * @return
 **/
func stageOut(ctx *cli.Context) error {

	dataDir := viper.Get(GKFSDATADIR)
	ldPreload := viper.Get(GKFSLDPRELOAD)
	workDir, err := os.Getwd()
	if err != nil {
		return err
	}
	shellCmd := fmt.Sprintf("mpirun -host `srun hostname|nodeset -f|nodeset -e | tr \" \" \",\"` -env LD_PRELOAD %s -env HOST_SIZE `srun hostname|nodeset -f|nodeset -c` -ppn 1 %s/stage-out %s %s %s/gkfs_hosts.txt.pid  %s",
		ldPreload,
		workDir,
		ctx.Args().Get(0),
		ctx.Args().Get(1),
		workDir,
		dataDir)
	fmt.Println(shellCmd)

	cmd := exec.Command("bash", "-c", shellCmd)

	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err = cmd.Run()
	if err != nil {
		log.Fatal("execute error ", err, " ", errb.String())
	}
	fmt.Println(outb.String())
	//fmt.Println(errb.String())
	return nil
}

func Copy(cCtx *cli.Context) error {

	if err := GenerateHostFile(); err != nil {
		log.Fatal("generate hostfile error")
	}

	arg1 := cCtx.Args().Get(0)
	arg2 := cCtx.Args().Get(1)
	mount := viper.GetString(GKFSMOUNTDIR)

	if strings.HasPrefix(arg1, mount) || strings.HasPrefix(arg2, mount) {
		if strings.HasPrefix(arg2, mount) {
			return stageIn(cCtx)
		} else {
			return stageOut(cCtx)
		}
	} else {
		log.Fatal("// The source and destination paths must include the GekkoFS mount path,\n                    // or please check the configuration for the correct mount path.")
	}
	return nil
}

func main() {
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".") // 还可以在工作目录中查找配置
	viper.SetConfigFile("config.yaml")
	//viper.SetConfigName("config")
	//viper.SetConfigType("yaml")
	viper.AutomaticEnv()

	//viper.AddConfigPath("/etc/gkfs/")    // 查找配置文件所在的路径
	//viper.AddConfigPath("$HOME/workdir") // 多次调用以添加多个搜索路径

	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	//TODO slurm mpi clush env check!

	app := &cli.App{

		Commands: []*cli.Command{
			{
				Name:    "cp",
				Aliases: []string{"-c"},
				Usage:   "copy file from lustre to gekkofs, or gekkofs to lustre",
				Action:  Copy,
			},
			{
				Name: "gen",
				//Aliases: []string{"-gg"},
				Usage: "generate host file  ",
				Action: func(cCtx *cli.Context) error {
					fmt.Println("only test")
					GenerateHostFile()
					return nil
				},
			},
			{
				Name:    "test",
				Aliases: []string{"t"},
				Usage:   "test subcommands scripts for prepare environment",
				Subcommands: []*cli.Command{
					{
						Name:   "test",
						Usage:  "test scripts for prepare environment",
						Action: prepareEnv,
					},
					{
						Name:  "remove",
						Usage: "remove an existing template",
						Action: func(cCtx *cli.Context) error {
							fmt.Println("removed task template: ", cCtx.Args().First())
							return nil
						},
					},
				},
			},
		},
	}
	app.Version = "0.0.2"
	app.Usage = "data transport tools for GekkoFS"
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
