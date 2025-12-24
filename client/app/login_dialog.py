from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QLineEdit, QPushButton, QMessageBox
)

from app.socket_client import SocketClient


class LoginDialog(QDialog):
    def __init__(self, client: SocketClient, parent=None):
        super().__init__(parent)
        self.client = client
        self.setWindowTitle("Login")

        self.login = QLineEdit()
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.Password)

        form = QFormLayout()
        form.addRow("Login", self.login)
        form.addRow("Password", self.password)

        self.btn = QPushButton("Sign in")
        self.btn.clicked.connect(self._do_login)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addWidget(self.btn)
        self.setLayout(layout)

    def _do_login(self):
        resp = self.client.login(self.login.text().strip(), self.password.text())
        if not resp.get("ok"):
            QMessageBox.critical(self, "Login failed", resp.get("error", "error"))
            return
        self.accept()
