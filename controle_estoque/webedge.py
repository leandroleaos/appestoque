import webbrowser


edge_path= r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

webbrowser.register('edge', None, webbrowser.BackgroundBrowser(edge_path))

webbrowser.get('edge').open_new('http://www.microsoft.com')