cwlVersion: v1.2
class: CommandLineTool
label: variant-workbench-script-runner
doc: |-
  Run your own script at variant workbench.
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: ShellCommandRequirement
- class: ResourceRequirement
  coresMin: 16
  ramMin: $(inputs.spark_driver_mem * 1000)
- class: DockerRequirement
  dockerPull: pgc-images.sbgenomics.com/qqlii44/pyspark:3.5.1
- class: InlineJavascriptRequirement

inputs:
- id: spark_driver_mem
  doc: GB of RAM to allocate to this task
  type: int?
  default: 48
  inputBinding:
    position: 3
    prefix: --spark_driver_mem
- id: spark_executor_instance
  doc: number of instances used 
  type: int?
  default: 3
  inputBinding:
    position: 3
    prefix: --spark_executor_instance
- id: spark_executor_mem
  doc: GB of executor memory
  type: int?
  default: 34
  inputBinding:
    position: 3
    prefix: --spark_executor_mem
- id: spark_executor_core
  doc: number of executor cores
  type: int?
  default: 5
  inputBinding:
    position: 3
    prefix: --spark_executor_core
- id: spark_driver_core
  doc: number of driver cores
  type: int?
  default: 2
  inputBinding:
    position: 3
    prefix: --spark_driver_core
- id: spark_driver_maxResultSize
  doc: GB of driver maxResultSize
  type: int?
  default: 2
  inputBinding:
    position: 3
    prefix: --spark_driver_maxResultSize
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  default: 36000
  inputBinding:
    position: 3
    prefix: --sql_broadcastTimeout
- id: python_script
  doc: the python script running in this app
  type: File
- id: input_file1
  type: File?
- id: input_file2
  type: File?
- id: input_tarred_file1
  type: File?
- id: input_tarred_file2
  type: File?
- id: input_folder1
  type: Directory?
- id: input_folder2
  type: Directory?
- id: clinvar
  type: boolean?
- id: consequences
  type: boolean?
- id: variants
  type: boolean?
- id: diagnoses
  type: boolean?
- id: phenotypes
  type: boolean?
- id: studies
  type: boolean?

outputs:
- id: app_output
  type: File?
  outputBinding:
    glob: '*.tsv.gz'

baseCommand: [/bin/bash, -c]
arguments:
  - position: 1
    valueFrom: |
      ${
        var parts = [];

        // OPTIONAL: print what will run (for debugging)
        parts.push("echo RUNNING COMMAND");

        // conditionally extract tar files
        if (inputs.input_tarred_file1) {
          parts.push("tar -xvf " + inputs.input_tarred_file1.path);
        }
        if (inputs.input_tarred_file2) {
          parts.push("tar -xvf " + inputs.input_tarred_file2.path);
        }

        // begin building Python command
        var pyCmd = "python3 " + inputs.python_script.path;

        // add optional single files
        if (inputs.input_file1)  { pyCmd += " --input_file1 "  + inputs.input_file1.path; }
        if (inputs.input_file2)  { pyCmd += " --input_file2 "  + inputs.input_file2.path; }
        if (inputs.input_folder1){ pyCmd += " --input_folder1 " + inputs.input_folder1.path; }
        if (inputs.input_folder2){ pyCmd += " --input_folder2 " + inputs.input_folder2.path; }

        // add exploded tar dirs
        if (inputs.input_tarred_file1) {
          pyCmd += " --input_tarred_file1 " + inputs.input_tarred_file1.nameroot.replace(".tar","") + "/";
        }
        if (inputs.input_tarred_file2) {
          pyCmd += " --input_tarred_file2 " + inputs.input_tarred_file2.nameroot.replace(".tar","") + "/";
        }

        // add boolean flags
        if (inputs.clinvar)      { pyCmd += " --clinvar"; }
        if (inputs.consequences) { pyCmd += " --consequences"; }
        if (inputs.variants)     { pyCmd += " --variants"; }
        if (inputs.diagnoses)    { pyCmd += " --diagnoses"; }
        if (inputs.phenotypes)   { pyCmd += " --phenotypes"; }
        if (inputs.studies)      { pyCmd += " --studies"; }

        parts.push(pyCmd);

        // combine with shell AND
        return parts.join(" && ");
      }
    shellQuote: false

