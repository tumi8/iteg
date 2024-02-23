---
layout: post
title: Graph-Pipeline
permalink: /graph-pipeline/
--- 

# Graph-Parsing Pipeline and Example Data

To enable reproducable results and help in understanding our approach we have open-sourced the full parsing pipeline that can create the ITEG from our paper.
Additionally, the repository contains an example graph created from the [Tranco](https://tranco-list.eu/) top 1M list that can be directly imported and explored via [Neo4J](https://neo4j.com/).
The code and data can be found [[here]({{ site.github_link }})] or directly via Git:

{% highlight bash %}
git clone {{ site.github_ssh }}
cd graph-pipeline
git lfs pull
{% endhighlight %}

Git-LFS is only used for the example graph (please decompress the data before usage).
For simplicity, we excluded the full IPv4 address space scan (performed with [ZMap](https://zmap.io/)) and the scan for an open port 443 on IPv6 Addresses with [zmapv6](https://github.com/topics/zmapv6) in this example.
In the following we explain the steps necessary to reproduce the `example-data/` from the repository.

## 1. Input Lists

The input for our ITEG was a domain name list and a full IPv4 address space scan.
However, we skipt the latter in this example and, instead, use the IP addresses from the blocklists as additional input. 
For this example, we used the [Tranco](https://tranco-list.eu/) top 1M domains, the [SSLBL](https://sslbl.abuse.ch/), and the [Feodo Tracker](https://feodotracker.abuse.ch/):

{% highlight bash %}
curl https://tranco-list.eu/download/W9K29/1000000 | cut -d , -f 2 > example-data/tranco-input.txt
curl https://sslbl.abuse.ch/blacklist/sslblacklist.csv > example-data/blocklists/abuse_ch_sslbl.csv
curl https://sslbl.abuse.ch/blacklist/sslipblacklist.csv > example-data/blocklists/abuse_ch_sslbl.ip.csv
curl https://feodotracker.abuse.ch/downloads/ipblocklist.csv > example-data/abuse_ch_feodo.ip.csv
{% endhighlight %}

## 2. DNS Resolutions

We have used a local [unbound](https://www.nlnetlabs.nl/projects/unbound/about/) server and [massdns](https://github.com/blechschmidt/massdns) for our large-scale DNS resolutions.
Any software would work that produces a CSV file like the following:

    172.217.16.206,google.com
    2a00:1450:4001:806::200e,google.com

We started with a list of domains names (`example-data/tranco-input.txt`), resolved them via massdns, and parsed the output into the proper format:

{% highlight bash %}
massdns  -o J -t AAAA -t A --filter NOERROR -r ./example-data/resolvers.txt \
    example-data/tranco-input.txt  | ./parse_massdns.py --output example-data/dns
{% endhighlight %}

This creates two files each for the IPv4 and IPv6 DNS resolutions.

## 3. The TLS Scan

Our TLS measruements were performed with the [TUM goscanner](https://github.com/tumi8/goscanner.git).
The scanner needs a single input file:

{% highlight bash %}
cat <(grep -F "#" -v example-data/blocklists/abuse_ch_sslbl.ip.csv | cut -d , --output-delimiter ":" -f 2,3) \
    <(grep -F "#" -v example-data/blocklists/abuse_ch_feodo.ip.csv | csvtool -u : col 2,3 - | tail -n +2) \
    example-data/dns/*.ipdomain | shuf > example-data/goscanner-input.csv
{% endhighlight %}

Then, we can start with the actual TLS measurements:

{% highlight bash %}
ulimit -n 1024000
goscanner -C example-data/goscanner.conf \
    --input-file example-data/goscanner-input.csv \
    --output example-data/tls
{% endhighlight %}


## 4. Combining it all as ITEG

We created the ITEG with [Apache Spark](https://spark.apache.org/).
Spark can be tricky to set up; hence, we provide docker scripts that set up the environment and parse the example data.
Have a look at the `docker-compose.yml` and the parsing script for further details.
The script extracts the scan dates from the directory name of the input.
Note the necessary cache directory that can be deleted after the run.

{% highlight bash %}
./create_graph.sh
{% endhighlight %}

## 5. Explore and Analyze the ITEG

Our parsing pipeline produces multiple files for each type of edge and node that can be analyzed with other tools.
For example, the CSV output under `example-data/ITEG` can be directly imported into Neo4J.

{% highlight bash %}
./import_neo4j.sh example-data/ITEG
{% endhighlight %}


We experienced that Neo4J provides a convenient Interface to manually explore the graph; however, for more advanced analyses we recommend tools like Apache Spark with the [GraphFrames](https://graphframes.github.io/graphframes/) library.

The output of the Neo4J schema function:

![Schema of the Example-Graph](/assets/example_schema.svg){:style="display:block; margin-left:auto; margin-right:auto"}

Note that there were IP Addresses embedded as Altertnative Name in some certificates. These were always self-signed certificates.
However, we did not consider such cases in the paper.
