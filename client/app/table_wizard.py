from __future__ import annotations

from typing import Any, Dict, List

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, QLineEdit, QPushButton,
    QComboBox, QCheckBox, QListWidget, QLabel, QMessageBox, QSpinBox
)
from PySide6.QtWidgets import QAbstractItemView


ALLOWED_TYPES = ["integer", "bigint", "varchar", "text", "bool", "timestamp", "date", "bytea"]
FILE_STORAGE_MODES = ["base64", "blob", "fs"]


class TableWizard(QDialog):
    """
    Create table wizard (fixed UX):
    - PK выбирается только чекбоксом "primary key" при добавлении колонки
    - file-column нельзя сделать PK
    - file-column = integer (file_id)
    - file metadata уходит в payload["columns"][i]["file"] = {storage_mode, required}
    """

    def __init__(self, tables: List[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create table")

        self.tables = tables
        self.columns: List[Dict[str, Any]] = []
        self.fks: List[Dict[str, Any]] = []
        self.uniques: List[List[str]] = []

        self.table_name = QLineEdit()
        self.table_name.setPlaceholderText("new_table_name")

        # column editor
        self.col_name = QLineEdit()
        self.col_type = QComboBox()
        self.col_type.addItems(ALLOWED_TYPES)

        self.col_len = QSpinBox()
        self.col_len.setRange(1, 2000)
        self.col_len.setValue(200)

        self.col_nullable = QCheckBox("nullable")
        self.col_nullable.setChecked(True)
        self.col_unique = QCheckBox("unique")

        # NEW: PK checkbox (source of truth)
        self.col_is_pk = QCheckBox("primary key")

        # file column UI
        self.col_is_file = QCheckBox("file column")
        self.col_storage = QComboBox()
        self.col_storage.addItems(FILE_STORAGE_MODES)
        self.col_required = QCheckBox("required file (NOT NULL)")

        self.add_col_btn = QPushButton("Add column")
        self.cols_list = QListWidget()

        # PK display (read-only)
        self.pk_list = QListWidget()
        self.pk_list.setSelectionMode(QAbstractItemView.NoSelection)

        # UNIQUE sets
        self.unique_list = QListWidget()
        self.unique_cols = QListWidget()
        self.unique_cols.setSelectionMode(QAbstractItemView.MultiSelection)
        self.add_unique_btn = QPushButton("Add UNIQUE(constraint)")

        # FK
        self.fk_col = QComboBox()
        self.fk_ref_table = QComboBox()
        # можно фильтровать системные, но оставим как было (кроме alembic_version)
        self.fk_ref_table.addItems([t for t in tables if t != "alembic_version"])
        self.fk_ref_col = QLineEdit("id")
        self.add_fk_btn = QPushButton("Add FK")
        self.fk_list = QListWidget()

        # actions
        self.create_btn = QPushButton("Create")
        self.cancel_btn = QPushButton("Cancel")

        # Layout
        root = QVBoxLayout()

        form = QFormLayout()
        form.addRow("Table name", self.table_name)
        root.addLayout(form)

        # Column section
        root.addWidget(QLabel("Columns"))
        row = QHBoxLayout()
        row.addWidget(QLabel("name"))
        row.addWidget(self.col_name)
        row.addWidget(QLabel("type"))
        row.addWidget(self.col_type)
        row.addWidget(QLabel("len(varchar)"))
        row.addWidget(self.col_len)
        row.addWidget(self.col_nullable)
        row.addWidget(self.col_unique)
        row.addWidget(self.col_is_pk)
        row.addWidget(self.col_is_file)
        row.addWidget(QLabel("storage"))
        row.addWidget(self.col_storage)
        row.addWidget(self.col_required)
        row.addWidget(self.add_col_btn)
        root.addLayout(row)
        root.addWidget(self.cols_list)

        # PK + uniques
        two = QHBoxLayout()

        pk_box = QVBoxLayout()
        pk_box.addWidget(QLabel("Primary key (set via checkbox per column)"))
        pk_box.addWidget(self.pk_list)

        un_box = QVBoxLayout()
        un_box.addWidget(QLabel("Unique constraints"))
        un_box.addWidget(QLabel("Select columns for UNIQUE:"))
        un_box.addWidget(self.unique_cols)
        un_box.addWidget(self.add_unique_btn)
        un_box.addWidget(QLabel("Added UNIQUE constraints:"))
        un_box.addWidget(self.unique_list)

        two.addLayout(pk_box, 1)
        two.addLayout(un_box, 1)
        root.addLayout(two)

        # FK section
        root.addWidget(QLabel("Foreign keys"))
        fk_row = QHBoxLayout()
        fk_row.addWidget(QLabel("column"))
        fk_row.addWidget(self.fk_col)
        fk_row.addWidget(QLabel("ref table"))
        fk_row.addWidget(self.fk_ref_table)
        fk_row.addWidget(QLabel("ref column"))
        fk_row.addWidget(self.fk_ref_col)
        fk_row.addWidget(self.add_fk_btn)
        root.addLayout(fk_row)
        root.addWidget(self.fk_list)

        # buttons
        btns = QHBoxLayout()
        btns.addStretch(1)
        btns.addWidget(self.create_btn)
        btns.addWidget(self.cancel_btn)
        root.addLayout(btns)

        self.setLayout(root)

        # hooks
        self.col_type.currentTextChanged.connect(self._on_type_change)
        self.col_is_file.toggled.connect(self._on_file_toggle)
        self.col_required.toggled.connect(self._on_required_toggle)

        self.add_col_btn.clicked.connect(self._add_column)
        self.add_fk_btn.clicked.connect(self._add_fk)
        self.add_unique_btn.clicked.connect(self._add_unique)
        self.create_btn.clicked.connect(self._validate_accept)
        self.cancel_btn.clicked.connect(self.reject)

        self._on_type_change(self.col_type.currentText())
        self._on_file_toggle(False)

    def _on_type_change(self, t: str):
        self.col_len.setEnabled(t == "varchar")

    def _on_required_toggle(self, checked: bool):
        # required file => NOT NULL
        if self.col_is_file.isChecked():
            self.col_nullable.setChecked(not checked)

    def _on_file_toggle(self, enabled: bool):
        self.col_storage.setEnabled(enabled)
        self.col_required.setEnabled(enabled)

        if enabled:
            # file column = integer file_id, and cannot be PK
            self.col_type.setCurrentText("integer")
            self.col_type.setEnabled(False)

            self.col_is_pk.setChecked(False)
            self.col_is_pk.setEnabled(False)

            # required toggles nullable
            self.col_nullable.setChecked(not self.col_required.isChecked())
        else:
            self.col_type.setEnabled(True)
            self.col_is_pk.setEnabled(True)

    def _refresh_lists(self):
        self.cols_list.clear()
        self.pk_list.clear()
        self.unique_cols.clear()
        self.fk_col.clear()

        pk_cols = [c["name"] for c in self.columns if c.get("_is_pk")]

        for c in self.columns:
            extra = []
            if not c["nullable"]:
                extra.append("NOT NULL")
            if c.get("unique"):
                extra.append("UNIQUE")
            if c["type"] == "varchar":
                extra.append(f'len={c["length"]}')
            if c.get("_is_pk"):
                extra.append("PK")
            if c.get("file"):
                extra.append(f'FILE[{c["file"]["storage_mode"]}]')
                if c["file"].get("required"):
                    extra.append("REQUIRED")

            self.cols_list.addItem(f'{c["name"]}: {c["type"]}' + ((" " + " ".join(extra)) if extra else ""))

            # для UNIQUE выбора
            self.unique_cols.addItem(c["name"])

            # для FK выбора
            self.fk_col.addItem(c["name"])

        # PK display
        for pkc in pk_cols:
            self.pk_list.addItem(pkc)

        # не оставлять авто-выделения
        self.unique_cols.clearSelection()
        self.unique_cols.setCurrentRow(-1)

    def _add_column(self):
        name = self.col_name.text().strip()
        if not name:
            QMessageBox.warning(self, "Column", "Column name required")
            return

        # простая проверка идентификатора
        if not name.replace("_", "").isalnum():
            QMessageBox.warning(self, "Column", "Bad column name (use letters/digits/underscore)")
            return

        if any(x["name"] == name for x in self.columns):
            QMessageBox.warning(self, "Column", "Column name must be unique")
            return

        is_file = self.col_is_file.isChecked()
        is_pk = self.col_is_pk.isChecked()

        if is_file and is_pk:
            QMessageBox.warning(self, "Column", "File column cannot be primary key")
            return

        t = self.col_type.currentText()

        col: Dict[str, Any] = {
            "name": name,
            "type": t,
            "nullable": self.col_nullable.isChecked(),
            "unique": self.col_unique.isChecked(),
        }

        if t == "varchar":
            col["length"] = int(self.col_len.value())

        # file metadata
        if is_file:
            storage_mode = self.col_storage.currentText()
            required = self.col_required.isChecked()

            if required and col["nullable"]:
                QMessageBox.warning(self, "File column", "Required file cannot be nullable")
                return

            col["file"] = {"storage_mode": storage_mode, "required": required}

            # РЕКОМЕНДАЦИЯ: file column обычно НЕ unique (можно реиспользовать file_id)
            # но не запрещаем, просто оставим как выбрал пользователь.
            # (если хочешь жёстко запретить — скажи)

        if is_pk:
            col["_is_pk"] = True
            col["nullable"] = False  # PK должен быть NOT NULL

        self.columns.append(col)

        # reset UI inputs
        self.col_name.clear()
        self.col_is_pk.setChecked(False)

        self._refresh_lists()

    def _add_fk(self):
        col = self.fk_col.currentText()
        if not col:
            QMessageBox.warning(self, "FK", "Add columns first")
            return

        # Нельзя добавлять FK для file-column вручную
        # (она логически должна ссылаться на files(id), но это лучше делать как обычный FK на files)
        # Если очень надо — можно разрешить, но по UX лучше запретить.
        src_col = next((c for c in self.columns if c["name"] == col), None)
        if src_col and src_col.get("file"):
            QMessageBox.warning(self, "FK", "Do not add FK for file-column manually (it should point to files.id)")
            return

        ref_table = self.fk_ref_table.currentText()
        ref_col = self.fk_ref_col.text().strip() or "id"

        fk = {
            "column": col,
            "ref_table": ref_table,
            "ref_column": ref_col,
            "on_delete": "CASCADE",
            "on_update": "CASCADE",
        }
        self.fks.append(fk)
        self.fk_list.addItem(f'{col} -> {ref_table}({ref_col})')

    def _add_unique(self):
        selected = [i.text() for i in self.unique_cols.selectedItems()]
        if not selected:
            QMessageBox.warning(self, "UNIQUE", "Select one or more columns")
            return

        # запрещаем UNIQUE-constraint, если в нём есть file-column (чтобы не ломать модель)
        file_cols = {c["name"] for c in self.columns if c.get("file")}
        if any(c in file_cols for c in selected):
            QMessageBox.warning(self, "UNIQUE", "Do not add UNIQUE constraint on file columns")
            return

        self.uniques.append(selected)
        self.unique_list.addItem("UNIQUE(" + ", ".join(selected) + ")")
        self.unique_cols.clearSelection()

    def _validate_accept(self):
        tname = self.table_name.text().strip()
        if not tname:
            QMessageBox.warning(self, "Create", "Table name required")
            return

        if not tname.replace("_", "").isalnum():
            QMessageBox.warning(self, "Create", "Bad table name (use letters/digits/underscore)")
            return

        if not self.columns:
            QMessageBox.warning(self, "Create", "At least one column required")
            return

        # PK only from checkbox per-column
        pk = [c["name"] for c in self.columns if c.get("_is_pk")]
        if not pk:
            QMessageBox.warning(self, "Create", "At least one primary key column required (use checkbox)")
            return

        self._pk = pk
        self.accept()

    def payload(self) -> Dict[str, Any]:
        pk = getattr(self, "_pk", [])
        # очистим внутренний флаг _is_pk, чтобы не мешал серверу (он не нужен DDL)
        cols = []
        for c in self.columns:
            c2 = dict(c)
            c2.pop("_is_pk", None)
            cols.append(c2)

        return {
            "table": self.table_name.text().strip(),
            "columns": cols,
            "primary_key": pk,
            "uniques": self.uniques,
            "foreign_keys": self.fks,
        }
