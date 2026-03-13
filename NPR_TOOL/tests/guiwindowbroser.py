import customtkinter as ctk
import webview # The new library

def open_browser():
    # This opens a separate, modern window using your PC's actual web engine
    webview.create_window('Modern Browser', 'https://www.digikey.com/?gclid=eb57bcc6d5ee13625972d572e023befd&gclsrc=3p.ds&msclkid=eb57bcc6d5ee13625972d572e023befd')
    webview.start()

root = ctk.CTk()
btn = ctk.CTkButton(root, text="Open Web Page", command=open_browser)
btn.pack(pady=20, padx=20)

root.mainloop()