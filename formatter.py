import json
import decimal
from xml.etree import ElementTree as ET
from xml.dom import minidom 


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


class OutputFormatter:
    def format(self, data: dict, format_type: str) -> str:
        if format_type == 'json':
            return self._to_json(data)
        elif format_type == 'xml':
            return self._to_xml(data)
        else:
            raise ValueError(f"Неподдерживаемый формат: {format_type}")

    def _to_json(self, data: dict) -> str:
        return json.dumps(data, indent=4, ensure_ascii=False, cls=DecimalEncoder)

    def _to_xml(self, data: dict) -> str:
        root = ET.Element("results")
        for query_name, records in data.items():
            query_element = ET.SubElement(root, query_name)
            for record in records:
                item_element = ET.SubElement(query_element, "item")
                for key, val in record.items():
                    if isinstance(val, decimal.Decimal):
                        val = str(val)
                    field_element = ET.SubElement(item_element, str(key))
                    field_element.text = str(val)

        xml_str = ET.tostring(root, 'utf-8')
        parsed_str = minidom.parseString(xml_str)
        return parsed_str.toprettyxml(indent="  ")