from fuzzywuzzy import fuzz

# Definisikan dua string
str1 = "5SDAbHEMtDLArMASFEN PHOTO STUDIO (PRA ORDER)"
str2 = "5SDA-HEM-DLA-MASFEN PHOTO STUDIO (PRA OR"

# Hitung rasio kesamaan
similarity_ratio = fuzz.ratio(str1, str2)
print(f"Similarity Ratio: {similarity_ratio}")