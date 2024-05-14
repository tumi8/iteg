---
layout: page
toc: Home
permalink: /
order: 1
title: "Providing Threat Intelligence with an Internet-wide TLS Ecosystem Graph Model"
description: "Additional material for the publication \"Propagating Threat Scores With a TLS Ecosystem Graph Model Derived by Active Measurements\", providing access to published data and tools."
---

Read our final version over ifip: **[[PDF]](https://tma.ifip.org/2024/wp-content/uploads/sites/13/2024/05/tma2024-final19.pdf)**

<div class="accordion-box">
  <div class="accordion-box__title">
    Abstract
  </div>
  <div class="accordion-box__content">
      <p>The Internet is shaped by independent actors and heterogeneous deployments. With the wide adoption of Transport Layer Security (TLS), a whole ecosystem of intertwined entities emerged. Acquiring a comprehensive view allows searching for previously unknown malicious entities and providing valuable cyber-threat intelligence. Actively collected Internet-wide Domain Name System (DNS) and TLS meta-data can provide the basis for such large-scale analyses. However, in order to efficiently navigate the vast volumes of data, an effective methodology is required. This work proposes a graph model of the TLS ecosystem that utilizes the relationships between servers, domains, and certificates. A Probabilistic Threat Propagation (PTP) algorithm is then used to propagate a threat score from existing blocklists to related nodes. We conducted a one-year-long measurement study of 13 monthly active Internet-wide DNS and TLS measurements to evaluate the methodology. The latest measurement found four highly suspicious clusters among the nodes with high threat scores. External threat intelligence services were used to confirm a high rate of maliciousness in the rest of the newly found servers. With the help of optimized thresholds, we identified 557 domains and 11 IP addresses throughout the last year before they were known to be malicious. Up to 40% of the identified nodes appeared on average three months later on the input blocklist. This work proposes a versatile graph model to analyze the TLS ecosystem and a PTP analysis to help security researchers focus on suspicious subsets of the Internet when searching for unknown threats.</p>
  </div>
</div><br>

**Authors:**
{% for author in site.data.authors.list %}<a style="border-bottom: none" href="https://orcid.org/{{author.orcid}}">
<img src="assets/ORCIDiD_icon16x16.png" style="width: 1em; margin-inline-start: 0.5em;" alt="ORCID iD icon"/></a>
[{{author.name}}](https://orcid.org/{{author.orcid}}){% if author.name contains "Sgan" %}{% else %}, {% endif %}
{% endfor %}


To supplement our paper, we provide the following additional contributions:

- A [graph parsing pipeline]({{ site.baseurl }}{% link _pages/pipeline.md %}) that can be used to construct an ITEG
- Example ITEG created from the Tranco top list and two blocklists
- The [message-passing based PTP implementation]({{ site.baseurl }}{% link _pages/ptp.md %}) used in the paper, and the computed scores on the example ITEG
- The [Figures]({{ site.baseurl }}{% link _pages/figures.md %}) from the paper as interactive plots. 


If you are referring to our work or use the collected data in your publication, you can use the following:

```bib
{% raw %}@article{sosnowski2024iteg,
  author = {Sosnowski, Markus and Sattler, Patrick and Zirngibl, Johannes and Betzer, Tim and Carle, Georg},
  title = {{Propagating Threat Scores With a TLS Ecosystem Graph Model Derived by Active Measurements}},
  booktitle = {Proc. Network Traffic Measurement and Analysis Conference (TMA)},
  year = 2024,
}{% endraw %}
```

