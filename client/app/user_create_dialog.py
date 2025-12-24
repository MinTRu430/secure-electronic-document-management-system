from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QLineEdit, QComboBox,
    QDialogButtonBox, QMessageBox
)
from app.socket_client import SocketClient


class UserCreateDialog(QDialog):
    def __init__(self, client: SocketClient, parent=None):
        super().__init__(parent)
        self.client = client
        self.setWindowTitle("Create user")

        self.login = QLineEdit()
        self.full_name = QLineEdit()
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.Password)

        self.role = QComboBox()
        self.role.addItems(["user", "admin"])

        form = QFormLayout()
        form.addRow("Login", self.login)
        form.addRow("Full name", self.full_name)
        form.addRow("Password", self.password)
        form.addRow("Role", self.role)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._create)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addWidget(buttons)
        self.setLayout(layout)

    def _create(self):
        login = self.login.text().strip()
        full_name = self.full_name.text().strip()
        password = self.password.text()
        role = self.role.currentText()

        resp = self.client.user_create(login, password, full_name, role)
        if not resp.get("ok"):
            QMessageBox.critical(self, "Error", resp.get("error", "user_create failed"))
            return

        QMessageBox.information(
            self,
            "OK",
            f"User created\n\nLogin: {login}\nPassword: {password}\n\nSave this password — it won't be shown again."
        )
        self.accept()

