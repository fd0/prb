Build:
```
go build -o prb .
```

Run:

```
TARGET=/home/user
for i in 1 2 3 4 5 6 7 8 9 10; do
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    ./prb --workers $i --output /tmp/benchmarks.csv "$TARGET"
done
```
