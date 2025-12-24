import sys
from PySide6.QtWidgets import QApplication

from app.ui_main import MainWindow


def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.resize(1100, 700)
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
