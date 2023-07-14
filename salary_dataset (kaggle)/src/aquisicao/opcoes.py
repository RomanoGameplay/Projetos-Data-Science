from enum import Enum
from .salary import SALARYETL


class EnumETL(Enum):
    salary = 'SALARY'


# Chave = Enum
# Valor = Classe de objeto ETL
ETL_DICT = {
    EnumETL.salary: SALARYETL
}
