rule download_data_ec16fadae3bd3f5420b81c7437cc4924:
  input:
    data:
      default: 1, 2, 3, 4, 5, 6, 7
  output:
  - out
  run: download_data

rule generate_report_ff8323ca0d6a46bdd812d0c778d65e27:
  input:
    data_file:
      source: process_data-13a6a8714cd84a40a84c6e50129c370f/out
  output:
  - out
  run: generate_report
  
rule process_data_13a6a8714cd84a40a84c6e50129c370f:
  input:
    data_file:
      source: download_data-ec16fadae3bd3f5420b81c7437cc4924/out
  output:
  - out
  run: process_data
