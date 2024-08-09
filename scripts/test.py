from fuzzywuzzy import fuzz

# Definisikan dua string
str1 = "5dprjbrpt3140040504957kepolisianripantai"
str2 = "5dprjbrpt3140040578642kepolisianri"

# Hitung rasio kesamaan
similarity_ratio = fuzz.ratio(str1, str2)
print(f"Similarity Ratio: {similarity_ratio}")