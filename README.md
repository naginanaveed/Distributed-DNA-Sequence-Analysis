# PDC Final Lab — Distributed DNA K-mer Mining Pipeline

![PySpark](https://img.shields.io/badge/Framework-Apache%20Spark%20%2F%20PySpark-orange?logo=apachespark)
![Python](https://img.shields.io/badge/Language-Python%203.x-blue?logo=python)
![Domain](https://img.shields.io/badge/Domain-Bioinformatics-green)
![Data](https://img.shields.io/badge/Data-NCBI%20RefSeq%20Bacterial%20Genomes-lightgrey)

---

## Overview

This project implements a **fully distributed DNA sequence analysis pipeline** using **Apache Spark (PySpark)** for the **Parallel and Distributed Computing (PDC) Final Lab Exam**. The pipeline ingests raw bacterial genome sequences in FASTA format from NCBI RefSeq, performs distributed feature extraction (GC content, nucleotide composition, k-mer frequencies), and produces comparative bioinformatics results across three bacterial organisms.

The pipeline is designed to scale from a local machine to a full cluster (YARN/Kubernetes) by simply adjusting Spark configuration parameters.

---

##  Objectives

- Download and parse real-world bacterial genome sequences from NCBI RefSeq (public domain)
- Build a complete ETL pipeline in PySpark: **Ingest → Clean → Transform → Analyze → Visualize**
- Demonstrate **distributed computing concepts**: data partitioning, RDD caching, UDFs, Window functions, and distributed aggregation
- Extract biologically meaningful sequence features: GC content, nucleotide composition, and tetranucleotide (k=4) k-mer frequencies
- Compare genomic composition across three diverse bacterial species
- Measure k-mer diversity using **Shannon Entropy**

---

##  Problem Statement

Bacterial genomes contain millions of nucleotides, and analysing their composition manually or sequentially is computationally prohibitive at scale. Key biological questions include:

- How does **GC content** differ across bacterial species, and what does it tell us about thermostability and adaptation?
- What are the **most frequent short DNA motifs (k-mers)** in each genome, and do they reveal restriction sites or repeat structures?
- How **compositionally diverse** is each genome, and does the Shannon entropy of k-mer distributions differ between organisms?
- Can a single **scalable Spark pipeline** answer all of these questions simultaneously across multiple genomes?

---

##  Why These Datasets?

Three well-studied, publicly available bacterial genomes were selected from **NCBI RefSeq** because they represent distinct GC content ranges and biological lifestyles — making comparative analysis biologically meaningful.

| Organism | NCBI Accession | GC Content | Biological Context |
|---|---|---|---|
| *Escherichia coli* K-12 MG1655 | GCF_000005845.2 | ~51% | Model gut bacterium; Enterobacteria reference genome |
| *Bacillus subtilis* 168 | GCF_000009045.1 | ~44% | Soil bacterium; model Firmicute, low-GC group |
| *Staphylococcus aureus* MRSA252 | GCF_000013425.1 | ~33% | Human pathogen; very low GC, AT-rich genome |

**Why these three specifically:**
- They span a wide GC range (33%–51%), enabling clear compositional comparison
- All are **complete genomes** (single chromosomes, no assembly gaps) — ideal for k-mer analysis
- All are **public domain** (NCBI/NIH), requiring no licensing
- Their biology is well-documented, allowing results to be validated against known literature

---

##  Dataset Details

| Property | Value |
|---|---|
| Source | NCBI RefSeq FTP server |
| File format | FASTA (.fna.gz — gzip compressed) |
| Approx. compressed size | ~4.6 MB per genome |
| Approx. uncompressed size | ~4–14 MB per genome |
| License | NCBI public domain (no restrictions) |
| Pre-processing | Only gzip decompression — no prior filtering |

---

##  Tech Stack

| Component | Tool / Library |
|---|---|
| Distributed computing | Apache Spark 3.x (PySpark) |
| Sequence parsing | Custom FASTA parser (Python) |
| Data manipulation | PySpark DataFrames, Window functions |
| Numerical computation | NumPy, Pandas |
| Visualisation | Matplotlib, Seaborn |
| Biological parsing support | BioPython (imported) |

---

##  Pipeline Workflow

```
NCBI RefSeq FASTA (.fna.gz)
         │
         ▼
  [STEP 1 — INGEST]
  urllib download + gzip decompress → .fna files
         │
         ▼
  [STEP 2 — PARSE]
  Custom FASTA parser → Python list of dicts
  {organism, seq_id, description, sequence}
         │
         ▼
  [STEP 3 — SPARK INGEST]
  createDataFrame → explicit schema → repartition(8, organism)
  DataFrame cached in memory
         │
         ▼
  [STEP 4 — CLEAN & VALIDATE]
  ├── Filter sequences shorter than 100 bp
  ├── Filter sequences with >5% ambiguous bases (N, R, Y, etc.)
  └── Strip all non-ACGT characters from sequence strings
         │
         ▼
  [STEP 5 — DISTRIBUTED FEATURE EXTRACTION]
  ├── UDF: gc_content_udf       → GC percentage per sequence
  ├── UDF: nucleotide_counts    → A / C / G / T counts per sequence
  └── UDF: extract_kmers + F.explode
          → one (kmer, count) row per kmer per sequence
         │
         ▼
  [STEP 6 — DISTRIBUTED AGGREGATION]
  ├── groupBy organism → genome summary (Table 1)
  ├── groupBy organism + kmer → ranked top k-mers (Table 2)
  ├── groupBy organism → nucleotide composition % (Table 3)
  └── Shannon entropy computed per organism (Table 4)
         │
         ▼
  [STEP 7 — VISUALISATION]
  ├── Plot 1: GC Content distribution (bar + KDE)
  ├── Plot 2: Nucleotide composition stacked bar
  ├── Plot 3: Top k-mer frequency heatmap
  └── Plot 4: K-mer Shannon entropy + unique k-mer count
         │
         ▼
  [STEP 8 — BIOLOGICAL INTERPRETATION + REPORT]
```

---

## Inputs

| Input | Description |
|---|---|
| `data/ecoli_k12.fna` | *E. coli* K-12 genome FASTA (auto-downloaded from NCBI) |
| `data/bsubtilis.fna` | *B. subtilis* 168 genome FASTA (auto-downloaded from NCBI) |
| `data/saureus.fna` | *S. aureus* MRSA252 genome FASTA (auto-downloaded from NCBI) |

All files are downloaded automatically by the notebook from the NCBI FTP server. No manual data preparation is required.

---

##  Outputs

All output files are written to the `results/` directory.

### CSV Tables

| File | Description |
|---|---|
| `results/table1_genome_summary.csv` | Per-organism genome statistics (sequence count, total bases, GC mean/stddev, sequence lengths) |
| `results/table2_top_kmers.csv` | Top 10 tetranucleotide k-mers per organism with counts and rank |
| `results/table3_nucleotide_composition.csv` | A / C / G / T percentages per organism |
| `results/table4_kmer_diversity.csv` | Shannon entropy, unique k-mer count, and relative entropy per organism |

### Plots (PNG, 150 DPI)

| File | Description |
|---|---|
| `results/plot1_gc_content.png` | Mean GC% bar chart with error bars + KDE density curves |
| `results/plot2_nucleotide_composition.png` | Stacked bar chart of nucleotide proportions (%) |
| `results/plot3_kmer_heatmap.png` | Heatmap of top-15 tetranucleotide frequencies (per 1,000 k-mers) |
| `results/plot4_kmer_diversity.png` | Shannon entropy bar + unique k-mer count bar per organism |

---

##  Results & Summary Tables

### Table 1 — Genome Summary Statistics per Organism

| Organism | Sequences | Total Bases | Mean GC (%) | StdDev GC | Min Seq Len | Max Seq Len | Mean Seq Len |
|---|---|---|---|---|---|---|---|
| B_subtilis | 1 | ~4,215,606 | ~43.5 | 0.0 | ~4,215,606 | ~4,215,606 | ~4,215,606 |
| E_coli_K12 | 1 | ~4,641,652 | ~50.8 | 0.0 | ~4,641,652 | ~4,641,652 | ~4,641,652 |
| S_aureus | 1 | ~2,902,619 | ~32.8 | 0.0 | ~2,902,619 | ~2,902,619 | ~2,902,619 |

> Each organism has one complete chromosome sequence in its RefSeq assembly. Exact values are computed at runtime and saved to CSV.

---

### Table 2 — Top 10 Tetranucleotides (k=4) per Organism

| Rank | E. coli K-12 | B. subtilis | S. aureus |
|---|---|---|---|
| 1 | AAAT / ATTT | AAAG / CTTT | AAAA / TTTT |
| 2 | Mixed-base kmers (high freq) | AT-rich + mixed | TTTT / AAAA |
| 3–10 | Balanced ACGT distribution | Moderate AT-enrichment | Strong AT bias (AAAT, TTTA, etc.) |

> Exact counts and all 10 k-mers per organism are saved to `results/table2_top_kmers.csv`.

---

### Table 3 — Nucleotide Composition per Organism (%)

| Organism | %A | %C | %G | %T |
|---|---|---|---|---|
| E. coli K-12 | ~24.6 | ~25.4 | ~25.4 | ~24.6 |
| B. subtilis | ~27.7 | ~22.2 | ~22.3 | ~27.8 |
| S. aureus | ~33.4 | ~16.7 | ~16.4 | ~33.5 |

> All organisms obey **Chargaff's rule** (A ≈ T, C ≈ G). S. aureus shows extreme AT enrichment (~67% A+T).

---

### Table 4 — K-mer Diversity (Shannon Entropy, k=4)

| Organism | Shannon H | Unique K-mers | Max Possible | Relative H (%) |
|---|---|---|---|---|
| E. coli K-12 | ~7.99 | 256 | 8.0 | ~99.9% |
| B. subtilis | ~7.97 | 256 | 8.0 | ~99.6% |
| S. aureus | ~7.88 | 256 | 8.0 | ~98.5% |

> All 256 possible tetranucleotides are present in all three genomes. S. aureus has slightly lower entropy, consistent with its AT-rich, less compositionally balanced genome.

---

##  Biological Interpretation

### 1. GC Content 
- **E. coli K-12** (~51% GC) is typical of Enterobacteria — moderate GC content, balanced genome
- **B. subtilis** (~44% GC) is characteristic of low-GC Firmicutes — soil bacterium adapted to environmental variation
- **S. aureus** (~33% GC) has very low GC content, typical of Staphylococci and consistent with its role as a human pathogen
- **Biological relevance:** GC-rich DNA forms more stable hydrogen bonds (3 per G-C pair vs. 2 per A-T pair), so high-GC organisms tend to be more thermostable. Low-GC genomes like S. aureus are often associated with host-adapted, pathogenic lifestyles


<img width="2761" height="985" alt="image" src="https://github.com/user-attachments/assets/363506ba-4f53-4778-b2c1-99d53f037d27" />

---
### 2. Nucleotide Composition 
- All three organisms confirm **Chargaff's rule** (A ≈ T and G ≈ C on double-stranded DNA)
- S. aureus shows the most extreme AT enrichment (~67% A+T), confirming its pathogenic adaptation and characteristic low-complexity genomic architecture


<img width="800" height="500" alt="image" src="https://github.com/user-attachments/assets/b0bf058f-59bc-45de-8e44-f3b56ef90068" />

---
### 3. K-mer Frequencies 
- Low-GC organisms (S. aureus) show dominance of homo-nucleotide runs like AAAA and TTTT in their top k-mers
- E. coli and B. subtilis exhibit more diverse top k-mers with mixed bases
- The k-mer GATC is a dam methylase recognition site; its frequency in E. coli has implications for DNA methylation and mismatch repair
- K-mer profiles can fingerprint organisms and are widely used in metagenomic classification

<img width="800" height="500" alt="image" src="https://github.com/user-attachments/assets/029fe8ac-240f-484f-b254-b95439aed3f1" />

---

### 4. K-mer Diversity / Shannon Entropy 
- All organisms approach **maximum possible Shannon entropy** (~8 bits for k=4, meaning 256 equally probable k-mers), indicating highly complex genomic sequences with no strong compositional depletion
- S. aureus's marginally lower entropy is consistent with its AT-rich, lower-complexity genomic background
- All 256 possible 4-mers are observed in all genomes — confirming the datasets represent complete, high-coverage assemblies
<img width="900" height="400" alt="image" src="https://github.com/user-attachments/assets/02f680f0-aec3-408c-8e4b-b9443041abf0" />

---

## ⚡ Distributed Computing Design Highlights

| Technique | Where Applied | Purpose |
|---|---|---|
| `repartition(8, "organism")` | After DataFrame creation | Distributes data across 8 partitions, partitioned by organism for data locality |
| `.cache()` | After cleaning, feature extraction | Avoids recomputation of expensive transformations across multiple downstream steps |
| Custom PySpark UDFs | GC content, nucleotide counts, k-mer extraction | Applies Python logic in a distributed, per-partition manner |
| `F.explode()` | K-mer rows expansion | Converts nested arrays to flat rows, enabling distributed aggregation |
| `Window.partitionBy().orderBy()` | K-mer ranking | Computes per-organism k-mer rank using Spark's distributed Window function |
| `groupBy().agg()` | All summary tables | Distributed shuffle-based aggregation across partitions |
| `spark.driver.memory = 4g` | Spark config | Adequate memory for genome-scale data processing locally |

**Scalability:** The same pipeline scales to GB-scale metagenomics datasets or thousands of genomes by increasing the number of partitions and deploying on a YARN or Kubernetes cluster — only the `SparkSession.master` setting changes.

---


---

## 📚 Dataset Citations

- **E. coli K-12 MG1655** — NCBI RefSeq GCF_000005845.2
  https://www.ncbi.nlm.nih.gov/datasets/genome/GCF_000005845.2/

- **B. subtilis 168** — NCBI RefSeq GCF_000009045.1
  https://www.ncbi.nlm.nih.gov/datasets/genome/GCF_000009045.1/

- **S. aureus MRSA252** — NCBI RefSeq GCF_000013425.1
  https://www.ncbi.nlm.nih.gov/datasets/genome/GCF_000013425.1/

All datasets are NCBI public domain. No preprocessing beyond gzip decompression was performed prior to pipeline execution.

---


