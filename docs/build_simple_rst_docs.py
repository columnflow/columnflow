from __future__ import annotations
import os
import sys

rst_template = """{title}
{underline}

.. currentmodule:: {modulename}
.. automodule:: {modulename}
    :autosummary:
    :members:
    :undoc-members:

"""

rst_toplevel_template = """{title}
{underline}

.. currentmodule:: {modulename}
.. automodule:: {modulename}
    :autosummary:
    :members:
    :undoc-members:

.. toctree::
    :maxdepth: 1

    {rst_files}
"""


def build_rst_file(
    output_path: str,
    file_path: str,
    rst_path: str | None = None,
) -> bool:
    """Build a rst file for a given python file.

    :param output_path: Output path for the rst file.
    :param file_path: Path of the python file to document.
    :return: Path of the generated rst file.
    """

    if "." in file_path:
        file_path = ".".join(file_path.split(".")[:-1])
    fname: str = os.path.basename(file_path)
    module_name: str = file_path.replace(os.sep, ".")
    title: str = F"``{fname}``"
    underline: str = "=" * len(title)
    rst: str = rst_template.format(
        title=title,
        underline=underline,
        modulename=module_name,
    )

    if not rst_path:
        rst_path = os.path.join(output_path, fname + ".rst")
    print(f"Writing rst file: {rst_path}")
    with open(rst_path, "w") as f:
        f.write(rst)

    return os.path.exists(rst_path)


def create_toplevel_rst_file(
        output_path: str,
        dirname: str,
        documented_modules: list[str],
) -> bool:
    """Create a toplevel rst file for a given directory.

    :param output_path: Output path for the rst file.
    :param dirname: Name of the directory to document.
    :param documented_modules: List of documented modules.
    :return: Path of the generated rst file.
    """

    title: str = "``{dirname}``".format(dirname=dirname.replace(os.sep, "."))
    underline: str = "=" * len(title)
    rst_files: str = "\n    ".join(documented_modules)
    rst: str = rst_toplevel_template.format(
        title=title,
        underline=underline,
        modulename=dirname.replace(os.sep, "."),
        rst_files=rst_files,
    )
    rst_path: str = os.path.join(output_path, "index.rst")
    with open(rst_path, "w") as f:
        f.write(rst)

    return os.path.exists(rst_path)


def main(output_path, *files):
    output_path = os.path.abspath(output_path)
    if not os.path.exists(output_path):
        print(f"Creating output path: {output_path}")
        os.makedirs(output_path)
    dirname = None
    documented_modules = list()
    documented_submodules = list()
    if len(files) == 1 and os.path.basename(files[0]) == "__init__.py":
        build_rst_file(
            output_path,
            file_path=os.path.dirname(files[0]),
            rst_path=os.path.join(output_path, "index.rst"),
        )
    for file in files:
        if os.path.isdir(file):
            subdir = os.path.basename(file)
            main(
                os.path.join(output_path, subdir),
                *os.listdir(file),
            )
            documented_submodules.append(f"{subdir}/index")
        elif os.path.basename(file).startswith("__"):
            continue
        else:
            this_dirname = os.path.dirname(file)
            if dirname is None:
                dirname = this_dirname
            elif dirname != this_dirname:
                raise ValueError(f"Files must have the same basename: {dirname} != {this_dirname}")

            if build_rst_file(output_path, file):
                documented_modules.append(os.path.basename(file).split(".")[0])
    documented_modules.extend(documented_submodules)
    if len(documented_modules) > 0:
        create_toplevel_rst_file(output_path, dirname, documented_modules)


if __name__ == "__main__":
    main(*sys.argv[1:])
