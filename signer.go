package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func ExecutePipeline(freeFlowJobs ...job) {
	wg := &sync.WaitGroup{}
	ch := make([]chan interface{}, len(freeFlowJobs)*2)
	for i, freeFlowJob := range freeFlowJobs {
		if i == 0 {
			ch[i] = make(chan interface{})
		}
		ch[i+1] = make(chan interface{})
		wg.Add(1)
		go func(freeFlowJob job, i int) {
			defer wg.Done()
			freeFlowJob(ch[i], ch[i+1])
			close(ch[i+1])
		}(freeFlowJob, i)

	}
	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	var data string
	first := make(map[int]interface{})
	second := make(map[int]interface{})
	start := time.Now()
	iteration := 0
	for v := range in {

		data = strconv.Itoa(v.(int))
		fmt.Printf("%v SingleHash data %v \n", data, data)

		wg.Add(1)
		go func(data string, iteration int) {
			defer wg.Done()
			crc32 := DataSignerCrc32(data)
			fmt.Printf("%v SingleHash crc32 %s \n", data, crc32)
			mu.Lock()
			first[iteration] = crc32
			if val, ok := second[iteration]; ok {
				out <- fmt.Sprintf("%v~%v", first[iteration], val)
			}
			mu.Unlock()
		}(data, iteration)

		wg.Add(1)
		go func(data string, iteration int) {
			defer wg.Done()
			mu.Lock()
			md5 := DataSignerMd5(data)
			mu.Unlock()
			fmt.Printf("%v SingleHash md5 %s \n", data, md5)
			crc32FromMD5 := DataSignerCrc32(md5)
			fmt.Printf("%v SingleHash crc32(md5) %s \n", data, crc32FromMD5)
			mu.Lock()
			second[iteration] = crc32FromMD5
			if val, ok := first[iteration]; ok {
				out <- fmt.Sprintf("%v~%v", val, second[iteration])
			}
			mu.Unlock()
		}(data, iteration)
		iteration++
	}
	end := time.Since(start)
	wg.Wait()
	fmt.Printf("SingleHash  %s \n", end)
}

func MultiHash(in, out chan interface{}) {
	const NumOfSteps = 6

	start := time.Now()
	var data string
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	collect := make(map[int][]string)
	iteration := 0
	for v := range in {
		mu.Lock()
		collect[iteration] = make([]string, NumOfSteps)
		mu.Unlock()
		data = v.(string)
		for th := 0; th < NumOfSteps; th++ {
			wg.Add(1)
			go func(th int, data string, iteration int, collect map[int][]string) {
				defer wg.Done()
				crc32 := DataSignerCrc32(strconv.Itoa(th) + data)
				mu.Lock()
				collect[iteration][th] = crc32
				fmt.Printf("%s MultiHash %v, %s \n", data, th, crc32)
				mu.Unlock()
			}(th, data, iteration, collect)
		}
		iteration++
	}
	wg.Wait()
	for i := 0; i < len(collect); i++ {
		result := strings.Join(collect[i], "")
		out <- result
	}
	end := time.Since(start)
	fmt.Printf("MultiHash  %s \n", end)
}

func CombineResults(in, out chan interface{}) {
	collect := make([]string, 0, len(in))
	for v := range in {
		collect = append(collect, v.(string))
	}
	sort.Strings(collect)
	result := strings.Join(collect, "_")
	fmt.Printf("CombineResults  %s \n", result)
	out <- result
}
