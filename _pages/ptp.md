---
layout: post
title: Probabilistic Threat Propagation (PTP)
permalink: /ptp/
---

For our paper, we combined our ITEG with a Probabilistic Threat Propagation (PTP) approach, defined by Carter et al. [DOI:10.1109/ICASSP.2013.6638196](https://doi.org/10.1109/ICASSP.2013.6638196).

# Message-passing based implementation of PTP on the ITEgraph

To enable reproducible results and help in understanding our approach we have open-sourced the PTP algorithm used in our paper.
The code and pre-computed data can be found [[here]({{ site.github_link }})] or directly via Git:

{% highlight bash %}
git clone {{ site.github_ssh }}
cd graph-pipeline
git lfs pull
{% endhighlight %}

Git-LFS is only used for the example data.

## Running the algorithm

The PTP algorithm can be run with docker and spark in the provided container.

{% highlight bash %}
./ptp.sh
{% endhighlight %}


