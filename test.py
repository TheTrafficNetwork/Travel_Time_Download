from tkinter import filedialog as fd

filename = None
filename = fd.askopenfilename(
    initialdir = "C:\\", 
    title = "Select a file.",
    filetypes = (("csv files","*.csv"),("all files","*.*"))
    )
print(filename)
if filename == "":
    print("filename is nothing")
