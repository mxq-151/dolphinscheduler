/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { h, markRaw, VNode, VNodeChild } from 'vue'
import { NIcon } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { ArrowUpOutlined, ArrowDownOutlined } from '@vicons/antd'
import type { ITaskPriorityOption, IJsonItem } from '../types'

export function useOaType(): IJsonItem {
  const { t } = useI18n()
  const options = markRaw([
    {
      label: 'B004',
      value: 'BOO4',
      icon: ArrowUpOutlined,
      color: '#ff0000'
    },
    {
      label: 'BO20',
      value: 'BO20',
      icon: ArrowUpOutlined,
      color: '#ff0000'
    },
    {
      label: 'S015',
      value: 'S015',
      icon: ArrowUpOutlined,
      color: '#EA7D24'
    }
  ])
  const renderOption = ({
    node,
    option
  }: {
    node: VNode
    option: ITaskPriorityOption
  }): VNodeChild =>
    h(node, null, {
      default: () => [
        h(
          NIcon,
          {
            color: option.color
          },
          {
            default: () => h(option.icon)
          }
        ),
        h('span', { style: { 'z-index': 1 } }, option.label as string)
      ]
    })
  return {
    type: 'select',
    field: 'oaType',
    name: '流程类型',
    options,
    validate: {
      required: true
    },
    props: {
      renderOption
    },
    value: 'BOO4'
  }
}
