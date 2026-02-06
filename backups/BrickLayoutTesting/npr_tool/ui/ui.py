# the idea of the UI is to be able to disaply things that are neccesary for creating the NPR from the BOM. the process as of now is tedious, 
# we take a number form the BOM, copy, paste into a ctrl+F in excel, does it exist then filter for the part number,
# then filter for key words int he descirption, thenn if osmething looks like it might be the hit, check that part number on
# the internet, if those dont match then you are out of luck. find the part online, create a new part number,go back into the inventory,
# check partnumbers and see if similar parts exsist, if yes see what partnumber id they have, if its a partnumber body that exissts then adjust the sufex
# if no then check the component numbering system sheet holding all part number body for general descirpiont htta it should follow, 
# if yes the main descriptors exsits, thn add the suffex,if no then make it ourselves and blah blah blahblah blah. So much MANUAL copy and pasting. 
# i want to remove the the whole schtick, but manual work is nessceary nonetheless. I just dont want to go back and forth between the inventory and the BOM, 
# i want to see if it exsists as a list, if yes per part move on. If no well shit out of luck look it up now. A biiiig bottleneck is the parseing. 
# So this being said, we need: to show all part information from the the BOM sheet ( the input sheet) on that part, 
# and if there is a hit aginst what we have in the inventory, then we show that all the information on that as well, BUT 
# we have to show it in a way that the informaiton matches in the same area, hopefully as close to each other as possible.
# in the inventory sheet, the manufacturing part number that is listed in the BOM is under the coulmn: VendorItem. i would need these next to each other to compare
# the inventory part number correlated to the BOM part would also need to be displayed if there is a HIT.
# things like qunitity of the item in the inventory and quanity needed form the bom also neeeds to tbe next to each other. 
# basicaly simple matching of information dsplayed reight next to each other formated pretty. A big issue with the current version of the NPR tool,
# is that it exports ony manufacturing part numbers that exsist in the inventory. a big problem with this is: if the part number exsists in the inventory from the BOM
# theres no need to inclus it in the New Parts Report sheet (lol becuase it not a new part)
# all this being said I need an inutitive UI that allows me to be able to compare QUICKLY, expand information, EXPORT sheets with built NPRS, buttons or tabs that allow
# functionality from the bricks (like the digi key part searcher). kinda like and NPR creating dynamic sheet creator, like a process of creating something
# we have our input, and wht our NPR should look like when we save, and each brick we interact with from our ui will edit the NPR in some way and at the end of the process we will have an
# NPR we can save, or an excel with all the input sheets and the last NPR for completly comparison. brainstorm with me.