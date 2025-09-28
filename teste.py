from thefuzz import fuzz
from thefuzz import process
# Comparação direta
nome1 = "Gol Linhas Aéreas"
nome2 = "GOL LINHAS AÉREAS S.A. (EX- VRG LINHAS AÉREAS S.A.)"
similaridade = fuzz.token_set_ratio(nome1, nome2)
print(f"Similaridade: {similaridade}%")