while(true)
    do
        curl localhost:8081/?type=waf\&count=15
        usleep 50000
        //curl localhost:8081/?type=vds\&count=19
        //usleep 50000
    done

