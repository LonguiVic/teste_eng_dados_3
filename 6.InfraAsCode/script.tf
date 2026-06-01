# Documentação de referência utilizada:
# https://registry.terraform.io/providers/-/aws/latest/docs/resources/glue_job


terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.46"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# Role assumida pelo Glue Job durante sua execução
resource "aws_iam_role" "glue_role" {

  name = "glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "glue.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    projeto = "teste_eng_dados"
  }
}

# Anexa a policy gerenciada da AWS necessária para execução de Glue Jobs
resource "aws_iam_role_policy_attachment" "glue_service_role" {

  role = aws_iam_role.glue_role.name

  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Criação do Glue Job responsável por executar o script da etapa 2
resource "aws_glue_job" "analise_clientes" {

  name        = "analise-clientes"

  description = "Job responsável pela análise de clientes"

  role_arn    = aws_iam_role.glue_role.arn

  glue_version = "5.0"

  worker_type = "G.1X" # 1 DPU (4 vCPUs, 16 GB de memória) / disco de 94 GB (aproximadamente 44 GB livres)

  number_of_workers = 10

  max_retries = 0

  timeout = 60

  execution_property {
    max_concurrent_runs = 1
  }

  command {

    name = "glueetl"

    script_location = "s3://bucket-scripts/2.AnaliseDados/analise.py"

    python_version = "3"
  }

  default_arguments = {
    "--job-language" = "python"
  }

  tags = {
    projeto = "teste_eng_dados"
  }
}