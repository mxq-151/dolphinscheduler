import { useI18n } from 'vue-i18n'
import type { IJsonItem } from '../types'

export function useOaRequestId(): IJsonItem {
  const { t } = useI18n()
  return {
    type: 'input',
    field: 'oaRequestId',
    name: '流程ID',
    props: {
      placeholder: '输入oa流程requestId',
      rows: 2,
      type: 'text'
    }
  }
}