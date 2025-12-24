# client/app/ui_main.py
from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, Optional, List

from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QListWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLineEdit,
    QLabel,
    QMessageBox,
    QDialog,
    QFormLayout,
    QDialogButtonBox,
    QComboBox,
    QFileDialog,
)

from app.socket_client import SocketClient
from app.table_wizard import TableWizard
from app.login_dialog import LoginDialog
from app.user_create_dialog import UserCreateDialog
from app.backups_dialog import BackupsDialog


class RowDialog(QDialog):
    """
    Универсальная форма Insert/Update.

    INLINE FILE MODEL:
      - физические колонки <base>_name и <base>_data скрываем
      - но при INSERT можем требовать выбор файла, если file-column required=true
    """

    def __init__(
        self,
        client: SocketClient,
        table: str,
        meta: Dict[str, Any],
        initial: Optional[Dict[str, Any]] = None,
        parent=None,
    ):
        super().__init__(parent)
        self.client = client
        self.table = table
        self.meta = meta
        self.initial = initial or {}
        self.widgets: Dict[str, QWidget] = {}

        # for required file(s) on INSERT
        self.file_paths_by_base: Dict[str, str] = {}  # base -> local path

        self.setWindowTitle(f"{'Edit' if initial else 'Add'}: {table}")

        form = QFormLayout()
        fk_map = {fk["column"]: fk for fk in meta.get("foreign_keys", [])}

        # inline file physical columns to hide: <base>_name and <base>_data
        file_cols = set()
        for fc in meta.get("file_columns", []) or []:
            if fc.get("name_column"):
                file_cols.add(fc["name_column"])
            if fc.get("data_column"):
                file_cols.add(fc["data_column"])

        for col in meta.get("columns", []):
            name = col["name"]

            # id + created_at обычно не даём редактировать
            if name in ("id", "created_at"):
                continue

            # inline file columns should not be edited as text
            if name in file_cols:
                continue

            if name in fk_map:
                fk = fk_map[name]
                combo = QComboBox()
                resp = self.client.fk_options(fk["ref_table"])
                if not resp.get("ok"):
                    raise RuntimeError(resp.get("error", "fk_options error"))
                items = resp["data"]["items"]
                combo.addItem("—", None)
                for it in items:
                    combo.addItem(f'{it["id"]} — {it["label"]}', it["id"])

                if name in self.initial and self.initial[name] is not None:
                    val = self.initial[name]
                    try:
                        val = int(val)
                    except Exception:
                        pass
                    idx = combo.findData(val)
                    if idx >= 0:
                        combo.setCurrentIndex(idx)

                self.widgets[name] = combo
                form.addRow(QLabel(name), combo)
            else:
                edit = QLineEdit()
                if name in self.initial and self.initial[name] is not None:
                    edit.setText(str(self.initial[name]))
                self.widgets[name] = edit
                form.addRow(QLabel(name), edit)

        # Inline file pickers:
        # - on INSERT: required files must be chosen (validated in MainWindow.add_row)
        # - on EDIT: allow replacing file(s) right inside the Edit dialog
        for fc in meta.get("file_columns", []) or []:
            base = str(fc.get("base") or "").strip()
            if not base:
                continue

            name_col = str(fc.get("name_column") or "").strip()
            required = bool(fc.get("required", False))

            row_w = QWidget()
            h = QHBoxLayout()
            h.setContentsMargins(0, 0, 0, 0)

            btn = QPushButton("Choose…")
            lab = QLabel()
            lab.setTextInteractionFlags(Qt.TextSelectableByMouse)

            # show current file name on EDIT (if exists)
            current_name = None
            if initial and name_col:
                current_name = (self.initial.get(name_col) or "").strip() or None

            if current_name:
                lab.setText(f"Current: {current_name}")
            else:
                lab.setText("No file selected")

            h.addWidget(btn)
            h.addWidget(lab, 1)
            row_w.setLayout(h)

            def make_pick(b: str, label: QLabel):
                def _pick():
                    path, _ = QFileDialog.getOpenFileName(self, f"Select file for {b}")
                    if not path:
                        return
                    self.file_paths_by_base[b] = path
                    label.setText(f"New: {os.path.basename(path)}")
                return _pick

            btn.clicked.connect(make_pick(base, lab))

            title = f"{base} (file)"
            if not initial and required:
                title = f"{base} (required file)"
            form.addRow(QLabel(title), row_w)


        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addWidget(buttons)
        self.setLayout(layout)

    def values(self) -> Dict[str, Any]:
        type_map = {c["name"]: (c.get("type") or "").lower() for c in self.meta.get("columns", [])}

        def parse_value(col: str, raw: Optional[str]) -> Any:
            if raw is None:
                return None
            s = raw.strip()
            if s == "":
                return None

            t = type_map.get(col, "")
            if t in ("integer", "bigint", "smallint"):
                try:
                    return int(s)
                except Exception:
                    return s
            if t in ("boolean",):
                sl = s.lower()
                if sl in ("true", "t", "1", "yes", "y"):
                    return True
                if sl in ("false", "f", "0", "no", "n"):
                    return False
                return s
            return s

        out: Dict[str, Any] = {}
        for name, w in self.widgets.items():
            if isinstance(w, QComboBox):
                out[name] = w.currentData()
            else:
                out[name] = parse_value(name, w.text())
        return out

    def required_file_bases(self) -> List[str]:
        bases: List[str] = []
        for fc in self.meta.get("file_columns", []) or []:
            if fc.get("required"):
                b = str(fc.get("base") or "").strip()
                if b:
                    bases.append(b)
        return bases

    def chosen_files(self) -> List[Dict[str, Any]]:
        """
        Returns list for SocketClient.insert_with_files:
          { "base": str, "path": str, "mime_type": None }
        """
        out: List[Dict[str, Any]] = []
        for b, p in self.file_paths_by_base.items():
            out.append({"base": b, "path": p, "mime_type": None})
        return out


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DB UI (Sockets)")

        self.client = SocketClient("127.0.0.1", 9090, timeout=120.0)

        self.tables = QListWidget()
        self.table = QTableWidget()

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search… (по текстовым полям)")
        self.search_btn = QPushButton("Search")
        self.reset_btn = QPushButton("Reset")

        self.add_btn = QPushButton("Add")
        self.edit_btn = QPushButton("Edit")
        self.del_btn = QPushButton("Delete")
        self.refresh_btn = QPushButton("Refresh")
        self.create_table_btn = QPushButton("Create table")

        # files (INLINE MODEL)
        self.upload_file_btn = QPushButton("Upload file")
        self.open_file_btn = QPushButton("Open file")
        self.replace_file_btn = QPushButton("Replace file")
        self.delete_file_btn = QPushButton("Delete file")

        self.create_user_btn = QPushButton("Create user")
        self.backups_btn = QPushButton("Backups")

        self.current_table: Optional[str] = None
        self.current_meta: Optional[Dict[str, Any]] = None

        left = QVBoxLayout()
        left.addWidget(QLabel("Tables"))
        left.addWidget(self.tables)

        right_top = QHBoxLayout()
        right_top.addWidget(self.search_edit)
        right_top.addWidget(self.search_btn)
        right_top.addWidget(self.reset_btn)

        right_btns = QHBoxLayout()
        right_btns.addWidget(self.add_btn)
        right_btns.addWidget(self.edit_btn)
        right_btns.addWidget(self.del_btn)
        right_btns.addStretch(1)
        right_btns.addWidget(self.refresh_btn)
        right_btns.addWidget(self.create_table_btn)
        right_btns.addWidget(self.create_user_btn)
        right_btns.addWidget(self.upload_file_btn)
        right_btns.addWidget(self.open_file_btn)
        right_btns.addWidget(self.replace_file_btn)
        right_btns.addWidget(self.delete_file_btn)
        right_btns.addWidget(self.backups_btn)

        right = QVBoxLayout()
        right.addLayout(right_top)
        right.addLayout(right_btns)
        right.addWidget(self.table)

        root = QHBoxLayout()
        root.addLayout(left, 1)
        root.addLayout(right, 3)

        w = QWidget()
        w.setLayout(root)
        self.setCentralWidget(w)

        self.tables.currentTextChanged.connect(self.on_table_selected)
        self.refresh_btn.clicked.connect(self.refresh)
        self.search_btn.clicked.connect(self.search)
        self.reset_btn.clicked.connect(self.refresh)

        self.add_btn.clicked.connect(self.add_row)
        self.edit_btn.clicked.connect(self.edit_row)
        self.del_btn.clicked.connect(self.delete_row)
        self.create_table_btn.clicked.connect(self.create_table)

        self.upload_file_btn.clicked.connect(self.upload_file)
        self.open_file_btn.clicked.connect(self.open_file)
        self.replace_file_btn.clicked.connect(self.replace_file)
        self.delete_file_btn.clicked.connect(self.delete_file)

        self.create_user_btn.clicked.connect(self.create_user)
        self.backups_btn.clicked.connect(self.open_backups)

        dlg = LoginDialog(self.client, parent=self)
        if dlg.exec() != QDialog.Accepted:
            raise SystemExit(0)

        self.load_tables()

    def show_err(self, title: str, msg: str):
        QMessageBox.critical(self, title, msg)

    def open_backups(self):
        dlg = BackupsDialog(self.client, parent=self)
        dlg.exec()

    def load_tables(self):
        resp = self.client.list_tables()
        if not resp.get("ok"):
            self.show_err("Error", resp.get("error", "list_tables failed"))
            return
        self.tables.clear()
        for t in resp["tables"]:
            self.tables.addItem(t)

    def on_table_selected(self, table_name: str):
        if not table_name:
            return
        self.current_table = table_name
        meta_resp = self.client.table_meta(table_name)
        if not meta_resp.get("ok"):
            self.show_err("Error", meta_resp.get("error", "table_meta failed"))
            return
        self.current_meta = meta_resp["meta"]
        self.refresh()

    def refresh(self):
        if not self.current_table:
            return
        resp = self.client.select(self.current_table, limit=200, offset=0)
        if not resp.get("ok"):
            self.show_err("Error", resp.get("error", "select failed"))
            return
        self.fill_table(resp["data"]["columns"], resp["data"]["rows"])

    def search(self):
        if not self.current_table:
            return
        q = self.search_edit.text().strip()
        if not q:
            self.refresh()
            return
        resp = self.client.search(self.current_table, q, limit=200, offset=0)
        if not resp.get("ok"):
            self.show_err("Error", resp.get("error", "search failed"))
            return
        self.fill_table(resp["data"]["columns"], resp["data"]["rows"])

    def fill_table(self, columns, rows):
        self.table.clear()
        self.table.setColumnCount(len(columns))
        self.table.setRowCount(len(rows))
        self.table.setHorizontalHeaderLabels(columns)

        for r_i, row in enumerate(rows):
            for c_i, val in enumerate(row):
                item = QTableWidgetItem("" if val is None else str(val))
                item.setFlags(item.flags() ^ Qt.ItemIsEditable)
                self.table.setItem(r_i, c_i, item)

        self.table.resizeColumnsToContents()

    def _get_headers(self) -> list[str]:
        return [self.table.horizontalHeaderItem(i).text() for i in range(self.table.columnCount())]

    def _get_selected_pk(self) -> Optional[Dict[str, Any]]:
        if not self.current_meta:
            return None
        pk_cols = self.current_meta.get("primary_key") or []
        if not pk_cols:
            return None

        row = self.table.currentRow()
        if row < 0:
            return None

        headers = self._get_headers()
        pk: Dict[str, Any] = {}
        for pkc in pk_cols:
            if pkc not in headers:
                return None
            idx = headers.index(pkc)
            pk_val = self.table.item(row, idx).text()
            pk[pkc] = int(pk_val) if pk_val.isdigit() else pk_val
        return pk

    def _row_has_file(self, fc: Dict[str, Any]) -> bool:
        row = self.table.currentRow()
        if row < 0:
            return False
        headers = self._get_headers()
        data_col = fc.get("data_column")
        if not data_col or data_col not in headers:
            return False
        idx = headers.index(data_col)
        txt = self.table.item(row, idx).text().strip()
        return bool(txt)

    def _current_file_columns(self) -> list[Dict[str, Any]]:
        return (self.current_meta or {}).get("file_columns", []) or []

    def _choose_file_column(self) -> Optional[Dict[str, Any]]:
        fcols = self._current_file_columns()
        if not fcols:
            return None
        if len(fcols) == 1:
            return fcols[0]

        msg = QMessageBox(self)
        msg.setWindowTitle("Choose file column")
        msg.setText("This table has multiple file columns. Choose one:")

        buttons = []
        for fc in fcols:
            label = f"{fc.get('base')} ({fc.get('storage_mode')})"
            b = msg.addButton(label, QMessageBox.AcceptRole)
            buttons.append((b, fc))
        msg.addButton("Cancel", QMessageBox.RejectRole)
        msg.exec()

        clicked = msg.clickedButton()
        for b, fc in buttons:
            if clicked == b:
                return fc
        return None

    # ===== CRUD =====

    def add_row(self):
        if not (self.current_table and self.current_meta):
            return

        dlg = RowDialog(self.client, self.current_table, self.current_meta, initial=None, parent=self)
        if dlg.exec() != QDialog.Accepted:
            return

        values = dlg.values()

        # If there are required file columns, we must insert with files in one operation
        required_bases = dlg.required_file_bases()
        chosen = dlg.chosen_files()
        chosen_bases = {f["base"] for f in chosen}

        missing = [b for b in required_bases if b not in chosen_bases]
        if missing:
            self.show_err("Insert", f"Required file(s) not selected: {', '.join(missing)}")
            return

        if required_bases:
            resp = self.client.insert_with_files(self.current_table, values, chosen)
        else:
            resp = self.client.insert(self.current_table, values)

        if not resp.get("ok"):
            self.show_err("Insert error", resp.get("error", "insert failed"))
            return

        self.refresh()

    def edit_row(self):
        if not (self.current_table and self.current_meta):
            return
        pk = self._get_selected_pk()
        if not pk:
            self.show_err("Edit", "Select a row (and table must have PK).")
            return

        # we don't allow editing file columns in RowDialog as text (use file pickers/buttons)
        initial = {}
        headers = self._get_headers()
        row = self.table.currentRow()
        for i, h in enumerate(headers):
            initial[h] = self.table.item(row, i).text()

        dlg = RowDialog(self.client, self.current_table, self.current_meta, initial=initial, parent=self)
        if dlg.exec() != QDialog.Accepted:
            return

        values = dlg.values()              # обычные поля (может быть пусто)
        chosen = dlg.chosen_files()        # выбранные файлы для замены (может быть пусто)

        # Если не меняем вообще ничего — нечего делать
        if not values and not chosen:
            self.show_err("Edit", "Nothing to update.")
            return

        # 1) Обновляем обычные поля ТОЛЬКО если они есть
        if values:
            resp = self.client.update(self.current_table, pk, values)
            if not resp.get("ok"):
                self.show_err("Update error", resp.get("error", "update failed"))
                return

        # 2) Заменяем файлы (если выбраны)
        for f in chosen:
            fr = self.client.file_attach(self.current_table, pk, f["base"], f["path"], mime_type=None)
            if not fr.get("ok"):
                self.show_err("Replace file error", fr.get("error", "file_attach failed"))
                self.refresh()
                return

        self.refresh()


    def delete_row(self):
        if not self.current_table:
            return
        pk = self._get_selected_pk()
        if not pk:
            self.show_err("Delete", "Select a row (and table must have PK).")
            return
        if QMessageBox.question(self, "Delete", f"Delete row {pk}?") != QMessageBox.Yes:
            return
        resp = self.client.delete(self.current_table, pk)
        if not resp.get("ok"):
            self.show_err("Delete error", resp.get("error", "delete failed"))
            return
        self.refresh()

    def create_table(self):
        resp = self.client.list_tables()
        if not resp.get("ok"):
            self.show_err("Error", resp.get("error", "list_tables failed"))
            return

        wiz = TableWizard(resp["tables"], parent=self)
        if wiz.exec() != QDialog.Accepted:
            return

        payload = wiz.payload()
        cr = self.client.create_table(payload)
        if not cr.get("ok"):
            self.show_err("Create table error", cr.get("error", "create_table failed"))
            return

        QMessageBox.information(self, "Created", "Table created!\n\nSQL:\n" + cr["data"]["sql"])
        self.load_tables()

    # ===== Files actions (existing rows) =====

    def upload_file(self):
        if not (self.current_table and self.current_meta):
            return

        pk = self._get_selected_pk()
        if not pk:
            self.show_err("File", "Select a row first (table must have PK).")
            return

        fc = self._choose_file_column()
        if not fc:
            self.show_err("File", "This table has no file column configured.")
            return

        path, _ = QFileDialog.getOpenFileName(self, "Select file")
        if not path:
            return

        resp = self.client.file_attach(self.current_table, pk, fc["base"], path, mime_type=None)
        if not resp.get("ok"):
            self.show_err("Upload", resp.get("error", "upload failed"))
            return

        QMessageBox.information(self, "Upload", "Attached")
        self.refresh()

    def open_file(self):
        if not (self.current_table and self.current_meta):
            return

        pk = self._get_selected_pk()
        if not pk:
            self.show_err("File", "Select a row first (table must have PK).")
            return

        fc = self._choose_file_column()
        if not fc:
            self.show_err("File", "This table has no file column configured.")
            return

        if not self._row_has_file(fc):
            self.show_err("File", "No file in this row.")
            return

        try:
            meta, data = self.client.file_get(self.current_table, pk, fc["base"])
        except Exception as e:
            self.show_err("Open", str(e))
            return

        name = meta.get("original_name") or f'{fc.get("base")}.bin'
        tmp_dir = tempfile.mkdtemp(prefix="dbui_")
        out_path = os.path.join(tmp_dir, name)
        with open(out_path, "wb") as f:
            f.write(data)

        QDesktopServices.openUrl(QUrl.fromLocalFile(out_path))

    def delete_file(self):
        if not (self.current_table and self.current_meta):
            return

        pk = self._get_selected_pk()
        if not pk:
            self.show_err("File", "Select a row first.")
            return

        fc = self._choose_file_column()
        if not fc:
            self.show_err("File", "This table has no file column configured.")
            return

        if fc.get("required"):
            self.show_err("File", f"File '{fc['base']}' is required (NOT NULL). Delete is запрещён.")
            return

        if not self._row_has_file(fc):
            self.show_err("File", "No file in this row.")
            return

        if QMessageBox.question(self, "Delete file", f"Remove file '{fc['base']}' from this row?") != QMessageBox.Yes:
            return

        dr = self.client.file_delete(self.current_table, pk, fc["base"])
        if not dr.get("ok"):
            self.show_err("File", dr.get("error", "file_delete failed"))
            return

        QMessageBox.information(self, "Delete file", "Deleted")
        self.refresh()

    def replace_file(self):
        if not (self.current_table and self.current_meta):
            return

        pk = self._get_selected_pk()
        if not pk:
            self.show_err("File", "Select a row first.")
            return

        fc = self._choose_file_column()
        if not fc:
            self.show_err("File", "This table has no file column configured.")
            return

        path, _ = QFileDialog.getOpenFileName(self, "Select new file")
        if not path:
            return

        if self._row_has_file(fc):
            if QMessageBox.question(self, "Replace", f"Replace file '{fc['base']}' in this row?") != QMessageBox.Yes:
                return

        resp = self.client.file_attach(self.current_table, pk, fc["base"], path, mime_type=None)
        if not resp.get("ok"):
            self.show_err("Replace", resp.get("error", "upload failed"))
            return

        QMessageBox.information(self, "Replace", "Replaced")
        self.refresh()

    # ===== admin =====
    def create_user(self):
        dlg = UserCreateDialog(self.client, parent=self)
        dlg.exec()

    def open_backups(self):
        dlg = BackupsDialog(self.client, parent=self)
        dlg.exec()
