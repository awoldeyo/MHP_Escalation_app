import sys
from PyQt5.QtWidgets import QMainWindow, QApplication, QFileDialog
from PyQt5.QtCore import QThread
from login import Ui_MainWindow

from cocoa import Connection
from escalation import EscalationReport


class Login(QThread):

    def __init__(self, username, password, status_bar):
        QThread.__init__(self)
        self.username = username
        self.password = password
        self.status_bar = status_bar
        self.status_bar.showMessage('Login pending. Please wait...')
        

    def __del__(self):
        self.wait()
        
    def run(self):
        self.connection = Connection(self.username, self.password)
        self.status_bar.showMessage(self.connection.status)


class StoreResults(QThread):

    def __init__(self, jira, filename, status_bar):
        QThread.__init__(self)
        self.filename = filename
        self.jira = jira
        self.status_bar = status_bar
        self.status_bar.showMessage('Creating report. Please wait...')
        

    def __del__(self):
        self.wait()
        
    def run(self):
        self.report = EscalationReport(self.jira, self.filename)
        self.status_bar.showMessage(f'Created Report: {self.filename}')


class Window(QMainWindow):

    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.initUI()
        self.show()
        
    def initUI(self):
        self.ui.statusBar.showMessage("Not connected!")
        self.ui.pushButton.clicked.connect(self.establish_connection)
        self.ui.pushButton_2.clicked.connect(self.generate_reports)
    
    def establish_connection(self):
        self.username = self.ui.username.text()
        self.password = self.ui.password.text()
        self.login_thread = Login(self.username, self.password, self.ui.statusBar)
        self.login_thread.start()
        
    def generate_reports(self):
        self.filename = self.saveFileDialog()
        if self.filename is not None:
            self.report_thread = StoreResults(self.login_thread.connection.jira, self.filename, self.ui.statusBar)
            self.report_thread.start()
        
    def saveFileDialog(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        fileName, _ = QFileDialog.getSaveFileName(self,"Save file as...","","Excel-Document (*.xlsx);;All Files (*)", options=options)
        if fileName and fileName.endswith('xlsx'):
            return fileName
        elif fileName:
            return fileName + '.xlsx'
        

if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = Window()
    w.show()
    sys.exit(app.exec_())