
from lxml import etree

def build_cbam_xml(records, output_path):
    root = etree.Element("CBAMReport")

    for r in records:
        p = etree.SubElement(root, "Product")
        for k, v in r.items():
            el = etree.SubElement(p, k)
            el.text = str(v)

    tree = etree.ElementTree(root)
    tree.write(output_path, pretty_print=True, xml_declaration=True, encoding="utf-8")
    return output_path
