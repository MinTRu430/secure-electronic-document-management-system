# client/app/backups_dialog.py
from __future__ import annotations

from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QListWidget,
    QPushButton,
    QMessageBox,
    QHBoxLayout,
    QCheckBox,
    QFormLayout,
    QSpinBox,
    QComboBox,
    QLabel,
    QWidget,
)

from app.socket_client import SocketClient


class BackupsDialog(QDialog):
    def __init__(self, client: SocketClient, parent=None):
        super().__init__(parent)
        self.client = client
        self.setWindowTitle("Backups (admin)")

        # ===== backups list =====
        self.list = QListWidget()
        self.refresh_btn = QPushButton("Refresh")
        self.create_btn = QPushButton("Create backup")
        self.restore_btn = QPushButton("Restore selected")

        btns = QHBoxLayout()
        btns.addWidget(self.refresh_btn)
        btns.addWidget(self.create_btn)
        btns.addWidget(self.restore_btn)

        # ===== auto backup schedule =====
        self.auto_enabled = QCheckBox("Enable automatic backups")

        self.hour = QSpinBox()
        self.hour.setRange(0, 23)

        self.minute = QSpinBox()
        self.minute.setRange(0, 59)

        self.tz = QComboBox()
        self.tz.addItems(["UTC", "Europe/Helsinki"])

        self.next_run = QLabel("-")
        self.next_run.setTextInteractionFlags(self.next_run.textInteractionFlags())

        self.load_sched_btn = QPushButton("Load schedule")
        self.save_sched_btn = QPushButton("Save schedule")

        form = QFormLayout()
        form.addRow(self.auto_enabled)
        form.addRow("Time (HH:MM):", self._time_widget())
        form.addRow("Timezone:", self.tz)
        form.addRow("Next run:", self.next_run)

        sched_btns = QHBoxLayout()
        sched_btns.addWidget(self.load_sched_btn)
        sched_btns.addWidget(self.save_sched_btn)
        sched_btns.addStretch(1)

        # ===== layout =====
        layout = QVBoxLayout()
        layout.addWidget(self.list)
        layout.addLayout(btns)

        layout.addSpacing(12)
        layout.addLayout(form)
        layout.addLayout(sched_btns)

        self.setLayout(layout)

        # ===== signals =====
        self.refresh_btn.clicked.connect(self.reload)
        self.create_btn.clicked.connect(self.create_backup)
        self.restore_btn.clicked.connect(self.restore_backup)

        self.load_sched_btn.clicked.connect(self.load_schedule)
        self.save_sched_btn.clicked.connect(self.save_schedule)

        # initial load
        self.reload()
        self.load_schedule()

    def _time_widget(self) -> QWidget:
        w = QWidget()
        h = QHBoxLayout(w)
        h.setContentsMargins(0, 0, 0, 0)

        self.hour.setSuffix(" h")
        self.minute.setSuffix(" min")

        h.addWidget(self.hour)
        h.addWidget(self.minute)
        h.addStretch(1)
        return w

    # ===== backups =====

    def reload(self):
        resp = self.client.backup_list()
        if not resp.get("ok"):
            QMessageBox.critical(self, "Error", resp.get("error", "backup_list failed"))
            return

        self.list.clear()
        for name in resp["data"]["backups"]:
            self.list.addItem(name)

    def create_backup(self):
        resp = self.client.backup_create()
        if not resp.get("ok"):
            QMessageBox.critical(self, "Error", resp.get("error", "backup_create failed"))
            return
        QMessageBox.information(self, "OK", f'Created: {resp["data"]["name"]}')
        self.reload()

    def restore_backup(self):
        item = self.list.currentItem()
        if not item:
            QMessageBox.warning(self, "Restore", "Select backup first")
            return
        name = item.text()

        if QMessageBox.question(
            self,
            "Restore",
            f"RESTORE {name}?\nThis will overwrite current DB/files!",
        ) != QMessageBox.Yes:
            return

        resp = self.client.backup_restore(name)
        if not resp.get("ok"):
            QMessageBox.critical(self, "Error", resp.get("error", "backup_restore failed"))
            return

        QMessageBox.information(self, "OK", f"Restored: {name}\nRestart UI if needed.")
        self.reload()

    # ===== schedule =====

    def load_schedule(self):
        resp = self.client.backup_schedule_get()
        if not resp.get("ok"):
            QMessageBox.critical(self, "Error", resp.get("error", "backup_schedule_get failed"))
            return

        s = resp.get("schedule", {})
        self.auto_enabled.setChecked(bool(s.get("enabled", True)))
        self.hour.setValue(int(s.get("hour", 2)))
        self.minute.setValue(int(s.get("minute", 0)))

        tz = str(s.get("timezone", "UTC"))
        idx = self.tz.findText(tz)
        if idx >= 0:
            self.tz.setCurrentIndex(idx)

        self.next_run.setText(s.get("next_run_time") or "-")

    def save_schedule(self):
        enabled = self.auto_enabled.isChecked()
        hour = int(self.hour.value())
        minute = int(self.minute.value())
        timezone = self.tz.currentText()

        resp = self.client.backup_schedule_set(enabled, hour, minute, timezone)
        if not resp.get("ok"):
            QMessageBox.critical(self, "Error", resp.get("error", "backup_schedule_set failed"))
            return

        s = resp.get("schedule", {})
        self.next_run.setText(s.get("next_run_time") or "-")
        QMessageBox.information(self, "Saved", "Backup schedule saved and applied.")
